libluxio_includedir = $(includedir)/luxio
libluxio_include_HEADERS = btree.h array.h data.h dbm.h util.h types.h config.h

EXTRA_DIR = example test ptest

dist-hook:
	for subdir in $(EXTRA_DIR); do \
		cp -rp $$subdir $(distdir); \
	done

