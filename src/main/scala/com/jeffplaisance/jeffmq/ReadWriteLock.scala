// Copyright 2010 Jeff Plaisance
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.

package com.jeffplaisance.jeffmq

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
