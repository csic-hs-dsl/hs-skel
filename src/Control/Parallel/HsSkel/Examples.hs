{-# LANGUAGE Arrows #-}

module Control.Parallel.HsSkel.Examples (execSkParSimple, execSkMapSimple, execSkMapChunk, execSkVecProdChunk)
where

import Control.Arrow(returnA)
import Control.Parallel.HsSkel
import Control.Parallel.HsSkel.Exec

import Prelude hiding (mapM, id, (.))

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

-- Este parece andar bien
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

-- Este parece andar bien
skMapSimple :: Skel [Integer] [Integer]
skMapSimple = proc st0 -> do
    let st1 = stMap (stFromList st0) (skPar $ skSeq doNothing)
        st2 = stMap st1 skSync
    skRed st2 (skSeq (\(o, i) -> i:o)) -<< []

-- Este parece andar bien
skMapChunk :: Skel [Integer] [Integer]
skMapChunk = proc st0 -> do
    let st1 = stMap (stChunk (stFromList st0) 1000) (skPar $ skSeq (map doNothing))
        st2 = stMap st1 skSync
    skRed st2 (skSeq (\(o, i) -> i ++ o)) -<< []

-- Este no anda bien
skVecProdChunk :: Skel ([Double], [Double]) Double
skVecProdChunk = proc (vA, vB) -> do
    let pairs = zip vA vB -- lazy
        st1 = stChunk (stFromList pairs) 1000000
        st2 = stMap st1 (skPar $ skSeq $ map (uncurry (*)))
        st3 = stMap st2 skSync
    skRed st3 (skSeq $ (\(o, l) -> o + (sum l))) -<< 0

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

execSkMapChunk :: IO()
execSkMapChunk = do
    print "inicio"
    res <- exec skMapChunk (take 4000 $ repeat 1000000)
    print "fin"
    print res


execSkVecProdChunk :: IO()
execSkVecProdChunk = do
    print "inicio"
    res <- exec skVecProdChunk ([0 .. 10000000], [6 .. 10000006])
    print "fin"
    print res