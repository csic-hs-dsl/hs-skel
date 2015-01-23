{-# LANGUAGE Arrows #-}

module Control.Parallel.HsSkel.Examples
where

import Control.Arrow
import Control.Parallel.HsSkel
import Control.Parallel.HsSkel.Exec

import Data.Numbers.Primes (isPrime)
import Data.List (transpose)


testSize :: Integer
testSize = 10000

ej1 = exec (skSeq (**(2 :: Float)) . skSeq (const 5)) ()

ej2 = exec (skSeq (**(2 :: Float)) . skSync . (skPar $ skSeq $ const 5)) ()

ej4 = exec (skRed (stMap (stGen (generator testSize) (1, 1, 0)) (skSeq (\(i, fi) -> (i, isPrime fi)))) (skSeq (reducer))) ([] :: [Integer])

ej5 = exec fibPrimesSk ([] :: [Integer])

fibPrimesSk = skRed (stMap (stMap fibPrimesGenSk fibPrimesConsSk) skSync) fibPrimesRedSk
fibPrimesGenSk = stGen (generator testSize) (1, 1, 0)
fibPrimesConsSk = skPar $ skSeq (\(i, fi) -> (i, isPrime fi))
fibPrimesRedSk = skSeq reducer

reducer (l, (_, False)) = l
reducer (l, (p, True)) = p:l

generator :: Integer -> (Integer, Integer, Integer) -> Maybe ((Integer, Integer), (Integer, Integer, Integer))
generator n (i, f1, f2) = if i < n
                then Just ((i, f1+f2), (i+1, f1+f2, f1))
                else Nothing


main = do
    print "inicio"
    --res <- exec ejSN (1,2)
    --res <- exec ejSNdo (1, 2)
    --res <- ej4
    --res <- exec ej5do ([] :: [Integer])
    --res <- exec skVecProd ([0 .. 100000], [6 .. 100006])
    --res <- exec skMatProd ([[1, 2, 3, 10], [4, 5, 6, 10], [7, 8, 9, 10]], [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]])
    {--let mA = [[1 .. 1000] | i <- [1 .. 1000]]
        mB = [[1 .. 1000] | i <- [1 .. 1000]]
    eval mA
    eval mB
    res <- exec skMatProd (mA, mB)
    --}
    --res <- exec skParSimple (100000000)
    res <- exec skMapSimple [1000000000, 1000000000, 1000000000, 1000000000]
    --res <- exec skVecProdChunk ([0 .. 10000000], [6 .. 10000006])
    print "fin"
    print res

float = id :: Float -> Float

ejSN = (skSeq (uncurry (+))) . (skSync *** skSync) . (skPar (skSeq (** float 2)) *** skPar (skSeq (+ float 5)))

ejSNdo = proc (x, y) -> do
    x' <- skPar (skSeq (** float 2)) -< x
    y' <- skPar (skSeq (+ float 5)) -< y
    fpair <- skPairF -< (x', y')
    fres <- skMapF (uncurry (+)) -< fpair
    res <- skSync -< fres
    returnA -<  res
    --x'' <- SkSync -< x'
    --y'' <- SkSync -< y'
    --returnA -<  x'' + y''

swap f a b = f b a

ej5do = proc x -> do
    ret <- skRed (stMap (stMap fibPrimesGenSk fibPrimesConsSk) skSync) fibPrimesRedSk -< x
    returnA -< ret


skVecProdChunkCaca :: Skel ([Double], [Double]) Double
skVecProdChunkCaca = proc (vA, vB) -> do
    let gen = stChunk (stMap (stFromList [0 .. length vA - 1]) (skSeq $ \i -> vA !! i * vB !! i)) 10000
    let gen2 = stMap gen $ skSeq sum
    skRed gen2 (skSeq $ uncurry (+)) -<< 0


skVecProd :: Skel ([Double], [Double]) Double
skVecProd = proc (vA, vB) -> do
    let pairs = zip vA vB -- lazy
        st1 = stFromList pairs
        st2 = stMap st1 (skSeq $ uncurry (*))
    skRed st2 (skSeq $ uncurry (+)) -<< 0


skMatProd :: Skel ([[Double]], [[Double]]) [[Double]]
skMatProd = proc (mA, mB) -> do 
    mBt <- skSeq transpose -< mB
    let rowLength = length mA
        resList = [sum $ zipWith (*) a b | a <- mA , b <- mBt]
        resStream = stFromList resList
        magic (((l:ls), len), a) = 
            if (len < rowLength) then
               ((a:l):ls, len + 1)
            else
                ([a]:(l:ls), 1)
    algo <- skRed resStream (skSeq magic) -<< ([[]], 0)
    returnA -< reverse $ map reverse $ fst algo


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
