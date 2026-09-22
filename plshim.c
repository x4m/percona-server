/*
 * plshim.c - power-loss simulation shim (LD_PRELOAD).
 *
 * Tracks every write to files under $PL_DIR and whether it has been made
 * durable by fsync/fdatasync (or by O_SYNC/O_DSYNC on the fd).  O_DIRECT
 * does not count: it bypasses the page cache, not the device write cache,
 * so an O_DIRECT write is as volatile as a buffered one until fsync.
 * Writes a text journal to $PL_LOG:
 *
 *   W <path> <off> <len> <undo_off> <undo_len>   write; old bytes saved in undo
 *   F <path>                                     fsync/fdatasync -> all pending durable
 *   T <path> <size>                              ftruncate
 *   D <path>                                     unlink
 *   R <old> <new>                                rename
 *
 * Old contents (pre-write image) are appended to the binary file $PL_UNDO so
 * an external tool can roll a non-durable write back to what was on disk
 * before ("stale" model), replace it with junk ("garbage"/torn model), or
 * mix per sector.  The external tool freezes the process (SIGSTOP), reads
 * the journal, kills the process and applies the loss model to the files.
 *
 * Build: gcc -O2 -shared -fPIC -o plshim.so plshim.c -ldl -lpthread
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#define MAXFD 65536

static int (*r_open)(const char *, int, ...);
static int (*r_open64)(const char *, int, ...);
static int (*r_openat)(int, const char *, int, ...);
static int (*r_openat64)(int, const char *, int, ...);
static int (*r_creat)(const char *, mode_t);
static int (*r_close)(int);
static int (*r_dup)(int);
static int (*r_dup2)(int, int);
static int (*r_dup3)(int, int, int);
static ssize_t (*r_write)(int, const void *, size_t);
static ssize_t (*r_pwrite)(int, const void *, size_t, off_t);
static ssize_t (*r_pwrite64)(int, const void *, size_t, off64_t);
static ssize_t (*r_writev)(int, const struct iovec *, int);
static ssize_t (*r_pwritev)(int, const struct iovec *, int, off_t);
static ssize_t (*r_pwritev64)(int, const struct iovec *, int, off64_t);
static ssize_t (*r_pwritev2)(int, const struct iovec *, int, off_t, int);
static ssize_t (*r_pwritev64v2)(int, const struct iovec *, int, off64_t, int);
static ssize_t (*r_pread)(int, void *, size_t, off_t);
static int (*r_fsync)(int);
static int (*r_fdatasync)(int);
static int (*r_ftruncate)(int, off_t);
static int (*r_ftruncate64)(int, off64_t);
static int (*r_unlink)(const char *);
static int (*r_unlinkat)(int, const char *, int);
static int (*r_rename)(const char *, const char *);
static int (*r_renameat)(int, const char *, int, const char *);

static struct
{
	char	   *path;			/* NULL = not tracked */
	int			durable;		/* O_SYNC/O_DSYNC: writes durable at once */
} fds[MAXFD];

static pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;
static char pl_dir[PATH_MAX];
static size_t pl_dir_len;
static int	logfd = -1;
static unsigned long long note_lsn;
static int	undofd = -1;
static off_t undo_off = 0;
static int	inited = 0;
static __thread int in_hook = 0;

#define LOAD(sym) r_##sym = dlsym(RTLD_NEXT, #sym)

static void
init(void)
{
	if (inited)
		return;
	LOAD(open); LOAD(open64); LOAD(openat); LOAD(openat64); LOAD(creat);
	LOAD(close); LOAD(dup); LOAD(dup2); LOAD(dup3);
	LOAD(write); LOAD(pwrite); LOAD(pwrite64); LOAD(writev);
	LOAD(pwritev); LOAD(pwritev64); LOAD(pwritev2); LOAD(pwritev64v2);
	LOAD(pread); LOAD(fsync); LOAD(fdatasync);
	LOAD(ftruncate); LOAD(ftruncate64);
	LOAD(unlink); LOAD(unlinkat); LOAD(rename); LOAD(renameat);

	const char *d = getenv("PL_DIR");
	const char *l = getenv("PL_LOG");
	const char *u = getenv("PL_UNDO");

	if (d && realpath(d, pl_dir))
	{
		pl_dir_len = strlen(pl_dir);
		if (l)
			logfd = r_open(l, O_WRONLY | O_CREAT | O_APPEND, 0644);
		if (u)
		{
			undofd = r_open(u, O_RDWR | O_CREAT, 0644);
			if (undofd >= 0)
				undo_off = lseek(undofd, 0, SEEK_END);
		}
	}
	inited = 1;
}

__attribute__((constructor)) static void ctor(void) { init(); }

static void
logf_(const char *fmt, ...)
{
	char		buf[3 * PATH_MAX];
	va_list		ap;
	int			n;

	if (logfd < 0)
		return;
	va_start(ap, fmt);
	n = vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);
	if (n > 0)
	{
		if (n > (int) sizeof(buf))
			n = sizeof(buf);
		r_write(logfd, buf, n);
	}
}

/* Returns malloc'd absolute path if under PL_DIR, else NULL. */
static char *
track_path(int dirfd, const char *path)
{
	char		abs[PATH_MAX];
	char		tmp[PATH_MAX];

	if (!pl_dir_len || !path)
		return NULL;
	if (path[0] != '/')
	{
		if (dirfd == AT_FDCWD)
		{
			if (!getcwd(tmp, sizeof(tmp)))
				return NULL;
		}
		else
		{
			char		lnk[64];
			ssize_t		k;

			snprintf(lnk, sizeof(lnk), "/proc/self/fd/%d", dirfd);
			k = readlink(lnk, tmp, sizeof(tmp) - 1);
			if (k < 0)
				return NULL;
			tmp[k] = 0;
		}
		if (strlen(tmp) + strlen(path) + 2 > sizeof(abs))
			return NULL;
		snprintf(abs, sizeof(abs), "%s/%s", tmp, path);
	}
	else
		snprintf(abs, sizeof(abs), "%s", path);

	/* normalize what we can; file may not exist yet, so do it by hand */
	{
		char		norm[PATH_MAX];
		char	   *out = norm;
		char	   *p = abs;

		*out = 0;
		while (*p)
		{
			char	   *q;
			size_t		seglen;

			while (*p == '/')
				p++;
			if (!*p)
				break;
			q = strchr(p, '/');
			seglen = q ? (size_t) (q - p) : strlen(p);
			if (seglen == 1 && p[0] == '.')
				;
			else if (seglen == 2 && p[0] == '.' && p[1] == '.')
			{
				char	   *s = strrchr(norm, '/');

				if (s)
					*s = 0, out = s;
			}
			else
			{
				*out++ = '/';
				memcpy(out, p, seglen);
				out += seglen;
				*out = 0;
			}
			p += seglen;
		}
		if (out == norm)
			strcpy(norm, "/");
		if (strncmp(norm, pl_dir, pl_dir_len) == 0 &&
			(norm[pl_dir_len] == '/' || norm[pl_dir_len] == 0))
			return strdup(norm);
	}
	return NULL;
}

static void
fd_set_(int fd, char *path, int flags)
{
	if (fd < 0 || fd >= MAXFD)
	{
		free(path);
		return;
	}
	pthread_mutex_lock(&mtx);
	free(fds[fd].path);
	fds[fd].path = path;
	fds[fd].durable = (flags & (O_SYNC | O_DSYNC)) != 0;
	pthread_mutex_unlock(&mtx);
}

static void
fd_clear(int fd)
{
	if (fd < 0 || fd >= MAXFD)
		return;
	pthread_mutex_lock(&mtx);
	free(fds[fd].path);
	fds[fd].path = NULL;
	pthread_mutex_unlock(&mtx);
}

/* Record a write of [off, off+len) to fd; must hold mtx; fd is tracked. */
static void
note_write(int fd, off_t off, size_t len)
{
	off_t		uoff = -1;
	ssize_t		ulen = 0;

	if (fds[fd].durable)
		return;
	if (undofd >= 0 && len > 0)
	{
		void	   *buf = NULL;

		/* data files may be O_DIRECT: the buffer must be sector aligned */
		if (posix_memalign(&buf, 4096, (len + 4095) & ~(size_t) 4095) != 0)
			buf = NULL;
		if (buf)
		{
			ulen = r_pread(fd, buf, len, off);	/* pre-image (we are before the write) */
			if (ulen < 0)
				ulen = 0;
			if (ulen > 0)
			{
				ssize_t		w = r_pwrite(undofd, buf, ulen, undo_off);

				if (w == ulen)
				{
					uoff = undo_off;
					undo_off += ulen;
				}
				else
					ulen = 0;
			}
			free(buf);
		}
	}
	logf_("W %s %lld %zu %lld %zd %llu\n", fds[fd].path, (long long) off, len,
		  (long long) uoff, ulen, note_lsn);
	note_lsn = 0;
}

/* ---- LSN extraction for the WAL-rule checker ---- */


static unsigned int be32(const unsigned char *p)
{ return ((unsigned int) p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3]; }
static unsigned long long be64(const unsigned char *p)
{ return ((unsigned long long) be32(p) << 32) | be32(p + 4); }

/* Redo block: end LSN of the block; data page: FIL_PAGE_LSN. Max over the buffer. */
static unsigned long long
lsn_of(int fd, const void *buf, size_t len, off_t off)
{
	const unsigned char *p = buf;
	unsigned long long m = 0;

	if (!p || !fds[fd].path)
		return 0;
	if (strstr(fds[fd].path, "#innodb_redo/"))
	{
		if (off < 2048 || (off % 512) != 0)
			return 0;
		for (size_t i = 0; i + 512 <= len; i += 512)
		{
			const unsigned char *b = p + i;
			unsigned int hdr = be32(b) & 0x7fffffffu;
			unsigned int dl = (be32(b + 4) >> 16) & 0x7fffu;
			unsigned int ep = be32(b + 8);
			unsigned long long l;

			if (hdr == 0 || ep == 0)
				continue;
			l = (((unsigned long long) (ep - 1) << 30) + (hdr - 1)) * 512 + dl;
			if (l > m)
				m = l;
		}
		return m;
	}
	if ((off % 16384) != 0)
		return 0;
	for (size_t i = 0; i + 16384 <= len; i += 16384)
	{
		unsigned long long l = be64(p + i + 16);

		if (l > m)
			m = l;
		if (!strstr(fds[fd].path, "#innodb_temp/") && !strstr(fds[fd].path, "ibtmp"))
			logf_("P %s %lld %u %u %llu\n", fds[fd].path,
				  (long long) (off + i), be32(p + i + 34), be32(p + i + 4), l);
	}
	return m;
}

static size_t
iov_len(const struct iovec *iov, int cnt)
{
	size_t		n = 0;

	for (int i = 0; i < cnt; i++)
		n += iov[i].iov_len;
	return n;
}

/* ---- open family ---- */

int
open(const char *path, int flags, ...)
{
	mode_t		mode = 0;
	int			fd;

	init();
	if (flags & (O_CREAT | O_TMPFILE))
	{
		va_list		ap;

		va_start(ap, flags);
		mode = va_arg(ap, mode_t);
		va_end(ap);
	}
	fd = r_open(path, flags, mode);
	if (fd >= 0 && !in_hook)
		fd_set_(fd, track_path(AT_FDCWD, path), flags);
	return fd;
}

int
open64(const char *path, int flags, ...)
{
	mode_t		mode = 0;
	int			fd;

	init();
	if (flags & (O_CREAT | O_TMPFILE))
	{
		va_list		ap;

		va_start(ap, flags);
		mode = va_arg(ap, mode_t);
		va_end(ap);
	}
	fd = r_open64(path, flags, mode);
	if (fd >= 0 && !in_hook)
		fd_set_(fd, track_path(AT_FDCWD, path), flags);
	return fd;
}

int
openat(int dirfd, const char *path, int flags, ...)
{
	mode_t		mode = 0;
	int			fd;

	init();
	if (flags & (O_CREAT | O_TMPFILE))
	{
		va_list		ap;

		va_start(ap, flags);
		mode = va_arg(ap, mode_t);
		va_end(ap);
	}
	fd = r_openat(dirfd, path, flags, mode);
	if (fd >= 0 && !in_hook)
		fd_set_(fd, track_path(dirfd, path), flags);
	return fd;
}

int
openat64(int dirfd, const char *path, int flags, ...)
{
	mode_t		mode = 0;
	int			fd;

	init();
	if (flags & (O_CREAT | O_TMPFILE))
	{
		va_list		ap;

		va_start(ap, flags);
		mode = va_arg(ap, mode_t);
		va_end(ap);
	}
	fd = r_openat64(dirfd, path, flags, mode);
	if (fd >= 0 && !in_hook)
		fd_set_(fd, track_path(dirfd, path), flags);
	return fd;
}

int
creat(const char *path, mode_t mode)
{
	int			fd;

	init();
	fd = r_creat(path, mode);
	if (fd >= 0)
		fd_set_(fd, track_path(AT_FDCWD, path), O_WRONLY);
	return fd;
}

int
close(int fd)
{
	init();
	fd_clear(fd);
	return r_close(fd);
}

int
dup(int fd)
{
	int			n;

	init();
	n = r_dup(fd);
	if (n >= 0 && fd >= 0 && fd < MAXFD && n < MAXFD)
	{
		pthread_mutex_lock(&mtx);
		free(fds[n].path);
		fds[n].path = fds[fd].path ? strdup(fds[fd].path) : NULL;
		fds[n].durable = fds[fd].durable;
		pthread_mutex_unlock(&mtx);
	}
	return n;
}

int
dup2(int fd, int nfd)
{
	int			n;

	init();
	n = r_dup2(fd, nfd);
	if (n >= 0 && fd >= 0 && fd < MAXFD && n < MAXFD && n != fd)
	{
		pthread_mutex_lock(&mtx);
		free(fds[n].path);
		fds[n].path = fds[fd].path ? strdup(fds[fd].path) : NULL;
		fds[n].durable = fds[fd].durable;
		pthread_mutex_unlock(&mtx);
	}
	return n;
}

int
dup3(int fd, int nfd, int flags)
{
	int			n;

	init();
	n = r_dup3(fd, nfd, flags);
	if (n >= 0 && fd >= 0 && fd < MAXFD && n < MAXFD && n != fd)
	{
		pthread_mutex_lock(&mtx);
		free(fds[n].path);
		fds[n].path = fds[fd].path ? strdup(fds[fd].path) : NULL;
		fds[n].durable = fds[fd].durable;
		pthread_mutex_unlock(&mtx);
	}
	return n;
}

/* ---- write family ---- */

#define TRACKED(fd) ((fd) >= 0 && (fd) < MAXFD && fds[fd].path && !in_hook)

ssize_t
write(int fd, const void *buf, size_t len)
{
	ssize_t		n;

	init();
	if (!TRACKED(fd))
		return r_write(fd, buf, len);
	pthread_mutex_lock(&mtx);
	in_hook = 1;
	{
		off_t		off = lseek(fd, 0, SEEK_CUR);

		if (off >= 0)
		{
			note_lsn = lsn_of(fd, buf, len, off);
			note_write(fd, off, len);
		}
		n = r_write(fd, buf, len);
	}
	in_hook = 0;
	pthread_mutex_unlock(&mtx);
	return n;
}

ssize_t
pwrite(int fd, const void *buf, size_t len, off_t off)
{
	ssize_t		n;

	init();
	if (!TRACKED(fd))
		return r_pwrite(fd, buf, len, off);
	pthread_mutex_lock(&mtx);
	in_hook = 1;
	note_lsn = lsn_of(fd, buf, len, off);
	note_write(fd, off, len);
	n = r_pwrite(fd, buf, len, off);
	in_hook = 0;
	pthread_mutex_unlock(&mtx);
	return n;
}

ssize_t
pwrite64(int fd, const void *buf, size_t len, off64_t off)
{
	ssize_t		n;

	init();
	if (!TRACKED(fd))
		return r_pwrite64(fd, buf, len, off);
	pthread_mutex_lock(&mtx);
	in_hook = 1;
	note_lsn = lsn_of(fd, buf, len, off);
	note_write(fd, off, len);
	n = r_pwrite64(fd, buf, len, off);
	in_hook = 0;
	pthread_mutex_unlock(&mtx);
	return n;
}

ssize_t
writev(int fd, const struct iovec *iov, int cnt)
{
	ssize_t		n;

	init();
	if (!TRACKED(fd))
		return r_writev(fd, iov, cnt);
	pthread_mutex_lock(&mtx);
	in_hook = 1;
	{
		off_t		off = lseek(fd, 0, SEEK_CUR);

		if (off >= 0)
			note_write(fd, off, iov_len(iov, cnt));
		n = r_writev(fd, iov, cnt);
	}
	in_hook = 0;
	pthread_mutex_unlock(&mtx);
	return n;
}

ssize_t
pwritev(int fd, const struct iovec *iov, int cnt, off_t off)
{
	ssize_t		n;

	init();
	if (!TRACKED(fd))
		return r_pwritev(fd, iov, cnt, off);
	pthread_mutex_lock(&mtx);
	in_hook = 1;
	note_write(fd, off, iov_len(iov, cnt));
	n = r_pwritev(fd, iov, cnt, off);
	in_hook = 0;
	pthread_mutex_unlock(&mtx);
	return n;
}

ssize_t
pwritev64(int fd, const struct iovec *iov, int cnt, off64_t off)
{
	ssize_t		n;

	init();
	if (!TRACKED(fd))
		return r_pwritev64(fd, iov, cnt, off);
	pthread_mutex_lock(&mtx);
	in_hook = 1;
	note_write(fd, off, iov_len(iov, cnt));
	n = r_pwritev64(fd, iov, cnt, off);
	in_hook = 0;
	pthread_mutex_unlock(&mtx);
	return n;
}

ssize_t
pwritev2(int fd, const struct iovec *iov, int cnt, off_t off, int flags)
{
	ssize_t		n;

	init();
	if (!TRACKED(fd))
		return r_pwritev2(fd, iov, cnt, off, flags);
	pthread_mutex_lock(&mtx);
	in_hook = 1;
	note_write(fd, off, iov_len(iov, cnt));
	n = r_pwritev2(fd, iov, cnt, off, flags);
	in_hook = 0;
	pthread_mutex_unlock(&mtx);
	return n;
}

ssize_t
pwritev64v2(int fd, const struct iovec *iov, int cnt, off64_t off, int flags)
{
	ssize_t		n;

	init();
	if (!TRACKED(fd))
		return r_pwritev64v2(fd, iov, cnt, off, flags);
	pthread_mutex_lock(&mtx);
	in_hook = 1;
	note_write(fd, off, iov_len(iov, cnt));
	n = r_pwritev64v2(fd, iov, cnt, off, flags);
	in_hook = 0;
	pthread_mutex_unlock(&mtx);
	return n;
}

/* ---- durability ---- */

int
fsync(int fd)
{
	int			rc;

	init();
	rc = r_fsync(fd);
	if (rc == 0 && TRACKED(fd))
	{
		pthread_mutex_lock(&mtx);
		logf_("F %s\n", fds[fd].path);
		pthread_mutex_unlock(&mtx);
	}
	return rc;
}

int
fdatasync(int fd)
{
	int			rc;

	init();
	rc = r_fdatasync(fd);
	if (rc == 0 && TRACKED(fd))
	{
		pthread_mutex_lock(&mtx);
		logf_("F %s\n", fds[fd].path);
		pthread_mutex_unlock(&mtx);
	}
	return rc;
}

int
ftruncate(int fd, off_t len)
{
	int			rc;

	init();
	rc = r_ftruncate(fd, len);
	if (rc == 0 && TRACKED(fd))
	{
		pthread_mutex_lock(&mtx);
		logf_("T %s %lld\n", fds[fd].path, (long long) len);
		pthread_mutex_unlock(&mtx);
	}
	return rc;
}

int
ftruncate64(int fd, off64_t len)
{
	int			rc;

	init();
	rc = r_ftruncate64(fd, len);
	if (rc == 0 && TRACKED(fd))
	{
		pthread_mutex_lock(&mtx);
		logf_("T %s %lld\n", fds[fd].path, (long long) len);
		pthread_mutex_unlock(&mtx);
	}
	return rc;
}

int
unlink(const char *path)
{
	int			rc;
	char	   *p;

	init();
	p = track_path(AT_FDCWD, path);
	rc = r_unlink(path);
	if (rc == 0 && p)
	{
		pthread_mutex_lock(&mtx);
		logf_("D %s\n", p);
		pthread_mutex_unlock(&mtx);
	}
	free(p);
	return rc;
}

int
unlinkat(int dirfd, const char *path, int flags)
{
	int			rc;
	char	   *p;

	init();
	p = track_path(dirfd, path);
	rc = r_unlinkat(dirfd, path, flags);
	if (rc == 0 && p)
	{
		pthread_mutex_lock(&mtx);
		logf_("D %s\n", p);
		pthread_mutex_unlock(&mtx);
	}
	free(p);
	return rc;
}

int
rename(const char *o, const char *n)
{
	int			rc;
	char	   *po,
			   *pn;

	init();
	po = track_path(AT_FDCWD, o);
	pn = track_path(AT_FDCWD, n);
	rc = r_rename(o, n);
	if (rc == 0 && (po || pn))
	{
		pthread_mutex_lock(&mtx);
		logf_("R %s %s\n", po ? po : o, pn ? pn : n);
		pthread_mutex_unlock(&mtx);
	}
	free(po);
	free(pn);
	return rc;
}

int
renameat(int od, const char *o, int nd, const char *n)
{
	int			rc;
	char	   *po,
			   *pn;

	init();
	po = track_path(od, o);
	pn = track_path(nd, n);
	rc = r_renameat(od, o, nd, n);
	if (rc == 0 && (po || pn))
	{
		pthread_mutex_lock(&mtx);
		logf_("R %s %s\n", po ? po : o, pn ? pn : n);
		pthread_mutex_unlock(&mtx);
	}
	free(po);
	free(pn);
	return rc;
}
