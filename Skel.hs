{-# LANGUAGE GADTs #-}
{-# LANGUAGE Arrows #-}

module Skel where

import Data.Traversable
import Control.Category
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.STM
import Control.Concurrent.STM.TQueue
import Control.DeepSeq (NFData, rnf)
import Control.Exception (evaluate, SomeException)
import Control.Monad hiding (mapM)
import Prelude hiding (mapM, id, (.))

import Data.Numbers.Primes (isPrime)

import Control.Arrow

import Data.List (transpose)



-- data del stream: Data d | Fin | Skip

newtype Future a = Future { mvar :: MVar a }

instance Show a => Show (Future a) where
    show _ = "Future ?"

instance NFData (Future a) where
    rnf _ = ()

queueLimit :: Int
queueLimit = 100000

eval :: (NFData a) => a -> IO a
eval a = do
    evaluate $ rnf a
    return a

infixr 9 `SkComp`
infixl 8 `SkPair`

data Skel i o where
    SkSeq   :: (NFData o) => (i -> o) -> Skel i o
    SkSeq_  :: (i -> o) -> Skel i o -- Use with caution!
    
    SkPar   :: Skel i o -> Skel i (Future o)
    SkSync  :: Skel (Future i) i
    SkComp  :: Skel x o -> Skel i x -> Skel i o
    SkPair  :: Skel i1 o1 -> Skel i2 o2 -> Skel (i1, i2) (o1, o2)

    SkMap   :: (Traversable t) => Skel i o -> Skel (t i) (t o)
    SkIf    :: Skel i o -> Skel i' o' -> Skel (Either i i') (Either o o')
    SkApply :: Skel (Skel i o, i) o

    SkRed   :: Stream i -> Skel (o, i) o -> Skel o o
    
    
data Stream d where
    StGen :: (NFData i, NFData o) => (i -> (Maybe (o, i))) -> i -> Stream o
    StMap :: Stream i -> Skel i o -> Stream o
    StChunk :: Stream i -> Integer -> Stream [i]

{- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ -}

instance Category Skel where
    id  = SkSeq_ id
    (.) = SkComp

instance Arrow Skel where
    arr = SkSeq_
    first sk = sk `SkPair` id
    second sk = id `SkPair` sk
    (***) = SkPair
    --(&&&) sk1 sk2 = idéntico a la impl. por defecto

instance ArrowChoice Skel where
    left sk = SkIf sk id
    right sk = SkIf id sk
    (+++) = SkIf
    --(|||) sk1 sk2 = idéntico a la impl. por defecto

instance ArrowApply Skel where
    app = SkApply

{- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ -}
-- Constructors
skSeq :: (NFData o) => (i -> o) -> Skel i o
skSeq = SkSeq

skPar :: Skel i o -> Skel i (Future o)
skPar = SkPar

skSync :: Skel (Future i) i
skSync = SkSync

skComp :: Skel x o -> Skel i x -> Skel i o
skComp = SkComp

skMap :: (Traversable t) => Skel i o -> Skel (t i) (t o)
skMap = SkMap

skRed :: Stream i -> Skel (o, i) o -> Skel o o
skRed = SkRed

stFromList :: (NFData a) => [a] -> Stream a
stFromList l = StGen go l
    where
        go [] = Nothing
        go (x:xs) = Just (x, xs)

{- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ -}
-- Util functions
skConst :: o -> Skel i o
skConst o = SkSeq_ (\_ -> o)

skMapF :: (NFData o) => (i -> o) -> Skel (Future i) (Future o)
skMapF fun = skPar $ (skSeq fun) . skSync

skPairF :: Skel (Future o1, Future o2) (Future (o1, o2))
skPairF = skPar $ (***) skSync skSync

skTraverseF :: (Traversable t) => Skel (t (Future o)) (Future (t o))
skTraverseF = skPar $ skMap skSync

skDaC :: (Traversable t) => Skel i o -> (i -> Bool) -> (i -> t i) -> (i -> t o -> o) -> Skel i o
skDaC skel isTrivial split combine = proc i -> do
    if (isTrivial i) then
        skel -< i
    else do
        oSplit <- skMap skSync . skMap (skPar (skDaC skel isTrivial split combine)) -< split i
        returnA -< combine i oSplit

{- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ -}

execStream :: Stream i -> IO (TBQueue (Maybe i))
execStream (StGen gen i) = do
    q <- newTBQueueIO queueLimit
    _ <- forkIO $ execGen q i
    return q
    where 
        execGen q i = do
            let res = gen i
            case res of 
                Just (v, i') -> do 
                    eval v
                    atomically $ writeTBQueue q (Just v)
                    execGen q i'
                Nothing -> atomically $ writeTBQueue q Nothing

--execStream (StMap (StMap stream' cons') cons) =  execStream (StMap stream' (cons . cons'))

execStream (StMap stream cons) = do
    qo <- newTBQueueIO queueLimit
    qi <- execStream stream
    _ <- forkIO $ consumer qi qo
    return qo
    where 
        consumer qi qo = do
            res <- atomically $ readTBQueue qi
            case res of 
                Just vi -> do 
                    --print ("StMap inicio ")-- ++ show vi)
                    vo <- exec cons vi
                    --print ("StMap fin ")-- ++ show vi)
                    atomically $ writeTBQueue qo (Just vo)
                    consumer qi qo
                Nothing -> atomically $ writeTBQueue qo Nothing
execStream (StChunk stream chunkSize) = do
    qo <- newTBQueueIO queueLimit
    qi <- execStream stream
    recc qi qo [] 0
    return qo
    where 
        recc qi qo ch cant = do
            i <- atomically $ readTBQueue qi
            case i of
                Just vi -> do
                    let newCh = vi:ch
                        newCant = cant + 1
                    if (newCant == chunkSize) 
                        then do
                            atomically $ writeTBQueue qo (Just $ reverse newCh)
                            recc qi qo [] 0
                        else do
                            recc qi qo newCh newCant
                Nothing -> do
                    atomically $ writeTBQueue qo (Just $ reverse ch)
                    atomically $ writeTBQueue qo Nothing


exec :: Skel i o -> i -> IO o
exec (SkSeq f) = (eval =<<) . liftM f . return
exec (SkSeq_ f) = liftM f . return
exec (SkPar sk) = \i -> (do
    mVar <- newEmptyMVar
    forkIO (stuff i mVar)
    return $ Future mVar)
    where
        stuff i mVar = do
            --print ("SkPar inicio " ++ show i)
            r <- exec sk i
            --print ("SkPar fin " ++ show i)
            putMVar mVar r
exec (SkSync) = takeMVar . mvar
exec (SkComp s2 s1) = (exec s2 =<<) . exec s1
exec (SkPair sk1 sk2) = \(i1, i2) -> do
    o1 <- exec sk1 i1
    o2 <- exec sk2 i2
    return (o1, o2)
exec (SkMap s) = mapM (exec s)
exec (SkIf sl sr) = \input ->
    case input of
        Left i -> exec sl i >>= return . Left
        Right i -> exec sr i >>= return . Right
exec (SkApply) = \(sk, i) -> exec sk i
exec (SkRed stream red) = \z -> do
    q <- execStream stream   
    reducer q z
    where 
        reducer q z = do
            res <- atomically $ readTBQueue q
            case res of
                Just v -> do
                    z' <- exec red (z, v)
                    reducer q z'
                Nothing -> return z  

{- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ -}
-- Tests:

testSize :: Integer
testSize = 10000

ej1 = exec (skSeq (**(2 :: Float)) . skSeq (const 5)) ()

ej2 = exec (skSeq (**(2 :: Float)) . skSync . (skPar $ skSeq $ const 5)) ()

ej4 = exec (skRed (StMap (StGen (generator testSize) (1, 1, 0)) (skSeq (\(i, fi) -> (i, isPrime fi)))) (skSeq (reducer))) ([] :: [Integer])

ej5 = exec fibPrimesSk ([] :: [Integer])

fibPrimesSk = skRed (StMap (StMap fibPrimesGenSk fibPrimesConsSk) skSync) fibPrimesRedSk
fibPrimesGenSk = StGen (generator testSize) (1, 1, 0)
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
    ret <- skRed (StMap (StMap fibPrimesGenSk fibPrimesConsSk) skSync) fibPrimesRedSk -< x
    returnA -< ret


skVecProdChunkCaca :: Skel ([Double], [Double]) Double
skVecProdChunkCaca = proc (vA, vB) -> do
    let gen = StChunk (StMap (stFromList [0 .. length vA - 1]) (skSeq $ \i -> vA !! i * vB !! i)) 10000
    let gen2 = StMap gen $ skSeq sum
    skRed gen2 (skSeq $ uncurry (+)) -<< 0


skVecProd :: Skel ([Double], [Double]) Double
skVecProd = proc (vA, vB) -> do
    let pairs = zip vA vB -- lazy
        st1 = stFromList pairs
        st2 = StMap st1 (skSeq $ uncurry (*))
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
    let st1 = StMap (stFromList st0) (skPar $ skSeq doNothing)
        st2 = StMap st1 skSync
    skRed st2 (skSeq (\(o, i) -> i:o)) -<< []

skVecProdChunk :: Skel ([Double], [Double]) Double
skVecProdChunk = proc (vA, vB) -> do
    let pairs = zip vA vB -- lazy
        st1 = StChunk (stFromList pairs) 1000000
        st2 = StMap st1 (skSeq $ map (uncurry (*)))
    skRed st2 (skSeq $ (\(o, l) -> o + (sum l))) -<< 0
