package jeffmq

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock.{ReadLock, WriteLock}

/**
 * @author jplaisance
 */
class ReadWriteLock(val fair:Boolean) extends java.util.concurrent.locks.ReadWriteLock {

    def this() = this(false)

    private val lock = new ReentrantReadWriteLock(fair)

    def writeLock:WriteLock = lock.writeLock

    def writeLock[A](work: =>A):A = {
        lock.writeLock.lock
        val a = work
        lock.writeLock.unlock
        a
    }

    def readLock:ReadLock = lock.readLock

    def readLock[A](work: =>A):A = {
        lock.readLock.lock
        val a = work
        lock.readLock.unlock
        a
    }
}
