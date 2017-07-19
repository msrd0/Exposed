package org.jetbrains.exposed.sql.tests.shared

import org.jetbrains.exposed.sql.vendors.*
import java.time.*
import kotlin.test.assertEquals

private fun<T> assertEqualCollectionsImpl(collection : Collection<T>, expected : Collection<T>) {
    assertEquals (expected.size, collection.size, "Count mismatch")
    for (p in collection) {
        assert(expected.any {p == it}) { "Unexpected element in collection pair $p" }
    }
}

fun<T> assertEqualCollections (collection : Collection<T>, expected : Collection<T>) {
    assertEqualCollectionsImpl(collection, expected)
}

fun<T> assertEqualCollections (collection : Collection<T>, vararg expected : T) {
    assertEqualCollectionsImpl(collection, expected.toList())
}

fun<T> assertEqualCollections (collection : Iterable<T>, vararg expected : T) {
    assertEqualCollectionsImpl(collection.toList(), expected.toList())
}

fun<T> assertEqualCollections (collection : Iterable<T>, expected : Collection<T>) {
    assertEqualCollectionsImpl(collection.toList(), expected)
}

fun<T> assertEqualLists (l1: List<T>, l2: List<T>) {
    assertEquals(l1.size, l2.size, "Count mismatch")
    for (i in 0..l1.size -1)
        assertEquals(l1[i], l2[i], "Error at pos $i:")
}

fun<T> assertEqualLists (l1: List<T>, vararg expected : T) {
    assertEqualLists(l1, expected.toList())
}

fun assertEqualDate (d1: LocalDate?, d2: LocalDate?) {
    if (d1 == null) {
        if (d2 != null)
            error("d1 is null while d2 is not")
        return
    } else {
        if (d2 == null)
            error("d1 is not null while d2 is null")
        
        assertEquals(d1.toEpochDay(), d2.toEpochDay())
    }
}

fun assertEqualDateTime (d1: ZonedDateTime?, d2: ZonedDateTime?) {
    if (d1 == null) {
        if (d2 != null)
            error("d1 is null while d2 is not")
        return
    } else {
        if (d2 == null)
            error ("d1 is not null while d2 is null")

        // Mysql doesn't support millis prior 5.6.4
        if (currentDialect == MysqlDialect && !MysqlDialect.isFractionDateTimeSupported()) {
            assertEquals(d1.toInstant().epochSecond, d2.toInstant().epochSecond)
        } else {
            assertEquals(d1.toInstant().toEpochMilli(), d2.toInstant().toEpochMilli())
        }
    }
}
