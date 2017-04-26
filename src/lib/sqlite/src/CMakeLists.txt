set(SRCDIR ${CMAKE_CURRENT_SOURCE_DIR})

add_custom_command(OUTPUT keywordhash.h
    DEPENDS mkkeywordhash
    COMMAND ../tool/mkkeywordhash > keywordhash.h)

add_custom_command(OUTPUT parse.h parse.c
    COMMAND lemon -DSQLITE_OMIT_VACUUM -T${SRCDIR}/../tool/lempar.c -oparse. ${SRCDIR}/parse.y
    COMMAND mv -f parse.h parse.h.tmp
    COMMAND ${TCLSH} ${SRCDIR}/../tool/addopcodes.tcl parse.h.tmp > parse.h
    COMMAND rm -f parse.h.tmp
)

add_custom_command(OUTPUT opcodes.h
    DEPENDS vdbe.c ../tool/mkopcodeh.tcl parse.h
    COMMAND cat parse.h ${SRCDIR}/vdbe.c | ${TCLSH} ${SRCDIR}/../tool/mkopcodeh.tcl > opcodes.h
)

add_custom_command(OUTPUT opcodes.c
    DEPENDS vdbe.c ../tool/mkopcodec.tcl parse.h opcodes.h
    COMMAND ${TCLSH} ${SRCDIR}/../tool/mkopcodec.tcl opcodes.h > opcodes.c
)

add_custom_command(OUTPUT sqlite3.h
    DEPENDS ../VERSION sqlite.h.in
    COMMAND ${TCLSH} ${SRCDIR}/../tool/mksqlite3h.tcl ${SRCDIR}/../ > sqlite3.h
)

add_definitions(-D_HAVE_SQLITE_CONFIG_H=1)
add_definitions(-DSQLITE_MAX_WORKER_THREADS=0)
add_definitions(-DSQLITE_OMIT_VACUUM=1)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_custom_target(generate_sqlite3_files DEPENDS
    parse.h
    keywordhash.h
    parse.h
    parse.c
    opcodes.c
    sqlite3.h)

set(TEST_DEFINITIONS
    SQLITE_NO_SYNC=1
    TCLSH=1
    SQLITE_TEST=1
    SQLITE_PRIVATE=
    SQLITE_CORE=1
)

include_directories(${TCL_INCLUDE_PATH} ${CMAKE_CURRENT_SOURCE_DIR})

add_library(trnsqlite
    # Generated files
    opcodes.c
    parse.c

    alter.c
    analyze.c
    attach.c
    auth.c
    backup.c
    bitvec.c
    btmutex.c
    btree.c
    build.c
    callback.c
    complete.c
    ctime.c
    date.c
    dbstat.c
    delete.c
    expr.c
    fault.c
    fkey.c
    func.c
    global.c
    hash.c
    insert.c
    legacy.c
    loadext.c
    main.c
    malloc.c
    mem0.c
    mem1.c
    mem2.c
    mem3.c
    mem5.c
    memjournal.c
    mutex.c
    mutex_noop.c
    mutex_unix.c
    mutex_w32.c
    notify.c
    os.c
    os_unix.c
    os_win.c
    pager.c
    pcache.c
    pcache1.c
    pragma.c
    prepare.c
    printf.c
    random.c
    resolve.c
    rowset.c
    select.c
    status.c
    table.c
    tokenize.c
    treeview.c
    trigger.c
    utf.c
    update.c
    util.c
    vdbe.c
    vdbeapi.c
    vdbeaux.c
    vdbeblob.c
    vdbemem.c
    vdbesort.c
    vdbetrace.c
    vtab.c
    wal.c
    walker.c
    where.c
    wherecode.c
    whereexpr.c
)
add_dependencies(trnsqlite generate_sqlite3_files)
set_target_properties(trnsqlite PROPERTIES COMPILE_DEFINITIONS
    "${TEST_DEFINITIONS}")

add_library(testfixture SHARED
    test1.c
    test2.c
    test3.c
    test4.c
    test5.c
    test6.c
    test7.c
    test8.c
    test9.c
    test_autoext.c
    test_async.c
    test_backup.c
    test_blob.c
    test_btree.c
    test_bestindex.c
    test_config.c
    test_demovfs.c
    test_devsym.c
    test_fs.c
    test_func.c
    test_hexio.c
    test_init.c
    test_intarray.c
    test_journal.c
    test_malloc.c
    test_multiplex.c
    test_mutex.c
    test_onefile.c
    test_osinst.c
    test_pcache.c
    test_quota.c
    test_rtree.c
    test_schema.c
    test_server.c
    test_superlock.c
    test_syscall.c
    test_tclvar.c
    test_thread.c
    test_vfs.c
    test_windirent.c
    test_wsd.c

    ../ext/misc/regexp.c
    ../ext/misc/totype.c
    ../ext/misc/wholenumber.c

    #
    # tclsqlite
    #
    tclsqlite.c
)
add_dependencies(testfixture generate_sqlite3_files)

if (APPLE)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -undefined suppress -flat_namespace")
endif(APPLE)

set_target_properties(testfixture PROPERTIES COMPILE_DEFINITIONS
    "${TEST_DEFINITIONS}")

target_link_libraries(testfixture ${TCL_LIBRARY})
