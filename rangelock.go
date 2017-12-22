package rangelock

import (
    "fmt"
    "errors"
    "sync"
)

const (
    READ = 1
    WRITE = 2
)

// implements the range locks for the object
type RangeLock struct {
    mutex *sync.Mutex
    cond  *sync.Cond
    lockedRanges map[uint64]*Range
}

// implements a particular range lock
type Range struct {
    startOffset uint64
    endOffset uint64
    readers []uint64    // list of readers on this range
    writer uint64       // exclusive writer on this range
}

// generic object that has range locks
type object struct {
    name string
    locks RangeLock     // locks on this object's ranges
}

// a map of object-ids to object
var objects map[uint64]*object

// initialize object
func (obj *object) init(name string) {
    obj.name = name
    obj.locks.mutex = &sync.Mutex{}
    obj.locks.cond = sync.NewCond(obj.locks.mutex)
    obj.locks.lockedRanges = make(map[uint64]*Range)
}

// initialize package
func Init() {
    objects = make(map[uint64]*object)
    // create a few objects
    objects[1] = &object{}
    objects[2] = &object{}
    objects[3] = &object{}
    // initialize objects
    for i, o := range objects {
        objName := fmt.Sprintf("ObjName%d", i)
        o.init(objName)
    }
}

// return true if startOff lies in the locked range
func (ra *Range) rangeConflict(startOff uint64) bool {
    if startOff >=  ra.startOffset && startOff <= ra.endOffset {
        return true
    }
    return false
}

// object lock interface
func (obj *object) lock(startOff, endOff uint64, rw int, ownerId uint64) (uint64, error) {
     // acquire mutex
     obj.locks.mutex.Lock()
     // defer release of mutex
     defer obj.locks.mutex.Unlock()
try_again:
     if rw == WRITE {
        for _, ra := range obj.locks.lockedRanges {
            if ra.rangeConflict(startOff) {
                // wait on waiters queue
                obj.locks.cond.Wait()
                // retry lock
                goto try_again
            }
        }
        // new range writer lock
        newra := &Range{}
        newra.startOffset = startOff
        newra.endOffset = endOff
        newra.writer = ownerId
        obj.locks.lockedRanges[startOff] = newra
        return startOff, nil
    } else if rw == READ {
        ra, prs  := obj.locks.lockedRanges[startOff]
        if prs {
           if  ra.startOffset == startOff && ra.endOffset == endOff {
                // exact match check for re-entrancy
                for _, rdrId := range ra.readers {
                    if rdrId == ownerId {
                        return startOff, nil
                    }
                }
                // add reader Id to readers
                ra.readers = append(ra.readers, ownerId)
                return startOff, nil
           }
        }
        // new range reader lock
        newra := &Range{}
        newra.startOffset = startOff
        newra.endOffset = endOff
        newra.writer = 0
        ra.readers = make([]uint64, 0)
        ra.readers = append(ra.readers, ownerId)
        return startOff, nil
    }
    return 0, errors.New("Unknown lock mode")
}

// object unlock interface
func (obj *object) unlock(lockId uint64, ownerId uint64) error {
    obj.locks.mutex.Lock()
    defer obj.locks.mutex.Unlock()
    // check lockedRanges for lockId
    ra, prs := obj.locks.lockedRanges[lockId]
    if !prs {
        return errors.New("Bad lockId")
    }
    // if exclusive
    if ra.writer != 0 {
       // then unset writer
       ra.writer = 0
   } else {
       // delete reader
       for i, rdrId := range ra.readers {
           if rdrId == ownerId {
               ra.readers = append(ra.readers, ra.readers[i+1:]...)
               break
           }
       }
   }
   // signal waiters and release mutex
   obj.locks.cond.Signal()
   return nil
}

// package Lock interface
func Lock(objId uint64, startOff, endOff uint64, rw int, ownerId uint64) (uint64, error) {
    _, prs := objects[objId]
    if prs {
        obj := objects[objId]
        return obj.lock(startOff, endOff, rw, ownerId)
    }
    return 0, errors.New("Invalid objId")
}

// package Unlock interface
func Unlock(objId uint64, lockId uint64, ownerId uint64) error {
    _, prs := objects[objId]
    if prs {
        obj := objects[objId]
        return obj.unlock(lockId, ownerId)
    }
    return errors.New("Invalid objId")
}
