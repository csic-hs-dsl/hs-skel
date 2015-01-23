{-# LANGUAGE Arrows #-}

module Control.Parallel.HsSkel.Examples
where

import Control.Arrow
import Control.Category (id, (.))
import Control.Parallel.HsSkel
import Control.Parallel.HsSkel.Exec

import Prelude hiding (mapM, id, (.))


testSize :: Integer
testSize = 10000


main :: IO ()
main = do
    print "inicio"
    --res <- exec skParSimple (100000000)
    res <- exec skMapSimple [1000000000, 1000000000, 1000000000, 1000000000]
    --res <- exec skVecProdChunk ([0 .. 10000000], [6 .. 10000006])
    print "fin"
    print res


doNothing :: Integer -> Integer
doNothing n = if n <= 0 
    then 0
    else doNothing (n-1)

skParSimple :: Skel (Integer) (Integer, Integer, Integer, Integer)
skParSimple = proc (a) -> do
    a' <- skPar (skSeq doNothing) -< a
    b' <- skPar (skSeq doNothing) -< a
    c' <- skPar (skSeq doNothing) -< a
    d' <- skPar (skSeq doNothing) -< a
    a'' <- skSync -< a'
    b'' <- skSync -< b'
    c'' <- skSync -< c'
    d'' <- skSync -< d'
    returnA -< (a'', b'', c'', d'')

skMapSimple :: Skel [Integer] [Integer]
skMapSimple = proc st0 -> do
    let st1 = stMap (stFromList st0) (skPar $ skSeq doNothing)
        st2 = stMap st1 skSync
    skRed st2 (skSeq (\(o, i) -> i:o)) -<< []

skVecProdChunk :: Skel ([Double], [Double]) Double
skVecProdChunk = proc (vA, vB) -> do
    let pairs = zip vA vB -- lazy
        st1 = stChunk (stFromList pairs) 1000000
        st2 = stMap st1 (skSeq $ map (uncurry (*)))
    skRed st2 (skSeq $ (\(o, l) -> o + (sum l))) -<< 0

