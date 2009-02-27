#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(luxio, 0.2.0, hiroyu@users.sourceforge.net)
AC_CONFIG_SRCDIR([btree.h])
AC_CONFIG_HEADER([config.h])
AM_INIT_AUTOMAKE(luxio, 0.2.0)

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_RANLIB

# Checks for libraries.
AC_PROG_LIBTOOL
AC_CHECK_LIB([pthread], [pthread_rwlock_init])

AC_CXX_HAVE_STD

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([fcntl.h stdint.h stdlib.h string.h sys/file.h sys/time.h unistd.h math.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_CONST
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_CHECK_MEMBERS([struct stat.st_blksize])
AC_HEADER_TIME
AC_SYS_LARGEFILE

# Checks for library functions.
AC_FUNC_MMAP
AC_CHECK_FUNCS([ftruncate getpagesize gettimeofday memmove memset mkdir munmap strchr pread pwrite])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
