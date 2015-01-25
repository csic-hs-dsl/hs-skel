{-# LANGUAGE Arrows #-}

module Control.Parallel.HsSkel.Examples (execSkParSimple, execSkMapSimple, execSkVecProdChunk)
where

import Control.Arrow(returnA)
import Control.Parallel.HsSkel
import Control.Parallel.HsSkel.Exec

import Prelude hiding (mapM, id, (.))


{-
main :: IO ()
main = do
    print "inicio"
    --res <- exec skParSimple (100000000)
    res <- exec skMapSimple [1000000000, 1000000000, 1000000000, 1000000000]
    --res <- exec skVecProdChunk ([0 .. 10000000], [6 .. 10000006])
    print "fin"
    print res
-}

{- ========================================================= -}
{- ======================== Utils ========================== -}
{- ========================================================= -}

doNothing :: Integer -> Integer
doNothing n = if n <= 0 
    then 0
    else doNothing (n - 1)



{- ============================================================== -}
{- ======================== Test Skels ========================== -}
{- ============================================================== -}

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

{- =============================================================== -}
{- ======================== Excel Tests ========================== -}
{- =============================================================== -}

execSkParSimple :: IO()
execSkParSimple = do
    print "inicio"
    res <- exec skParSimple (1000000000)
    print "fin"
    print res

execSkMapSimple :: IO()
execSkMapSimple = do
    print "inicio"
    res <- exec skMapSimple [1000000000, 1000000000, 1000000000, 1000000000]
    print "fin"
    print res

execSkVecProdChunk :: IO()
execSkVecProdChunk = do
    print "inicio"
    res <- exec skVecProdChunk ([0 .. 10000000], [6 .. 10000006])
    print "fin"
    print res