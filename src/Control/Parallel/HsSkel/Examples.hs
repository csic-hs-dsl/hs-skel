{-# LANGUAGE Arrows #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Control.Parallel.HsSkel.Examples (
    execSkForkSimple, 
    execSkMapSimple, 
    execSkMapChunk, 
    execSkMapChunkUnChunk,
    execSkMapChunkUnChunkStop,
    execSkMapChunkUnChunkStopIneff,
    execSkMapSkelSimple, 
    execSkVecProdChunk, 
    execSkKMeansOneStep,
    execSkKMeans,
    execSkQuicksort,
    execSkFibonacciPrimes,
    fibonacciPrimes,
    execKMeansTest
) where

import Control.Arrow(returnA, arr)
import Control.Category ((.))
import Control.Parallel.HsSkel
import Control.Parallel.HsSkel.Exec.Default

import Data.List (sort, find, concat)
import Data.Maybe (isNothing)
import qualified Data.Vector as V (Vector, filter, fromList, length, concat, head, tail, toList, singleton) 

import Prelude hiding ((.), concat, id, mapM, fmap, foldl1)
import qualified Prelude as P

import System.Random (randomRs)
import System.Random.TF.Init (mkTFGen)

--import Control.DeepSeq (NFData, rnf)
--import Control.Exception (evaluate)

--import Math.NumberTheory.Primes.Testing (isPrime, isCertifiedPrime)

import Debug.Trace (trace)

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
-- Usa: skFork, skStrict, skSync
skForkSimple :: Skel f Integer (Integer, Integer, Integer, Integer)
skForkSimple = proc a -> do
    a' <- (skFork doNothing) -< a
    b' <- (skFork doNothing) -< a
    c' <- (skFork doNothing) -< a
    d' <- (skFork doNothing) -< a
    a'' <- skSync -< a'
    b'' <- skSync -< b'
    c'' <- skSync -< c'
    d'' <- skSync -< d'
    returnA -< (a'', b'', c'', d'')

-- Este parece andar bien
-- Usa: skFork, skStrict, skSync, stMap, stFromList
skMapSimple :: Skel f [Integer] [Integer]
skMapSimple = proc l -> do
    let st = stMap skSync . stMap (skFork doNothing) . stFromList Z $ l
    skRed (arr (\(o, i) -> i : o)) [] -< st

-- Este parece andar bien
-- Usa: skFork, skStrict, skSync, stMap, stFromList, stChunk
skMapChunk :: Skel f [Integer] [Integer]
skMapChunk = proc l -> do
    let st = stParMap (skStrict doNothing) . stChunk 1000 . stFromList Z $ l
    skRed (arr (\(o, i) -> i : o)) [] -< st

-- Este parece andar bien
-- Usa: skFork, skStrict, skSync, stMap, stFromList, stChunk, skUnChunk
-- Ojo que desordena la lista!
skMapChunkUnChunk :: Skel f [Integer] [Integer]
skMapChunkUnChunk = proc l -> do
    let st = stUnChunk . stParMap (skStrict doNothing) . stChunk 1000 . stFromList Z $ l
    skRed (arr (\(o, i) -> i : o)) [] -< st
    
-- Este parece andar bien
-- Usa: skFork, skStrict, skSync, stMap, stFromList, stChunk, skUnChunk, stUntil
-- Ojo que desordena la lista!
skMapChunkUnChunkStop :: Skel f [Integer] [Integer]
skMapChunkUnChunkStop = proc l -> do
    let st = stUnChunk . stParMap (skStrict doNothing) . stChunk 1000 . stUntil (arr $ \(acc, _) -> acc + 1 :: Integer) 0 (arr (== 500000)) . stUnfoldr Z listUnfoldr $ l
    skRed (arr (\(o, i) -> i : o)) [] -< st
    where
        listUnfoldr :: [Integer] -> (Integer, [Integer])
        listUnfoldr (a:as) = (a, as)
        listUnfoldr _ = undefined -- La lista debe ser infinita

-- Este parece andar bien
-- Usa: skFork, skStrict, skSync, stMap, stFromList, stChunk, skUnChunk, stUntil
-- Ojo que desordena la lista!
-- Hay que tener en cuanta que esta es una solucion ineficiente, ya que el Stop está al final, es sólo para probar su propagación
skMapChunkUnChunkStopIneff :: Skel f [Integer] [Integer]
skMapChunkUnChunkStopIneff = proc l -> do
    let st = stUntil (arr $ \(acc, _) -> acc + 1 :: Integer) 0 (arr (== 500000)) . stUnChunk . stParMap (skStrict doNothing) . stChunk 1000 . stUnfoldr Z listUnfoldr $ l
    skRed (arr (\(o, i) -> i : o)) [] -< st
    where
        listUnfoldr :: [Integer] -> (Integer, [Integer])
        listUnfoldr (a:as) = (a, as)
        listUnfoldr _ = undefined -- La lista debe ser infinita

-- Este parece andar bien
-- Usa: skFork, skStrict, skSync, skMap, skTraverseF
skMapSkelSimple :: Skel f [Integer] [Integer]
skMapSkelSimple = proc l -> do 
    lf <- skMap (skFork doNothing) -< l
    skSync . skTraverseF -< lf

-- Este no anda bien
skVecProdChunk :: Skel f ([Double], [Double]) Double
skVecProdChunk = proc (vA, vB) -> do
    let pairs = zip vA vB -- lazy
        st = stMap skSync . stMap (skFork $ uncurry (*)) . stChunk 10000000 . stFromList Z $ pairs
    skRed (skStrict $ uncurry (+)) 0 -< st

dist :: (Floating a) => (a, a) -> (a, a) -> a
dist (x, y) (x', y') = (x - x') ** 2 + (y - y') ** 2

skKMeansOneStep :: Int -> Int -> Skel f ([(Double, Double)], [(Double, Double)], Integer) [(Double, Double)]
skKMeansOneStep assignChunk calcMeansChunk = proc (ps, ms, k) -> do
    ptgs <- calcPointGroup -< (ps, ms)
    ms' <- calcNewMeans -< (k, ptgs)
    returnA -< ms'
        where
            -- A partir de los puntos y las medias calcula a que grupo (índice de ms) pertenece cada punto
            calcPointGroup = proc (ps, ms) -> do
               let aux p = (p,
                           snd $ P.foldl1
                               (\(d, i) (d', i') -> if (d < d') then (d, i) else (d', i'))
                               (zipWith (\m i -> (dist p m, i)) ms [0 .. ]))

               let res = stParMap (skStrict aux) . stChunk assignChunk . stFromList Z $ ps

               -- Invierte la lista, pero no es problema
               skRed (arr (\(o, i) -> i : o)) [] -< res

            -- Sabiendo a que grupo pertenece cada punto, calcula la media de cada grupo. Asume que cada grupo tiene al menos un punto
            calcNewMeans = proc (k, ptgs) -> do
               let aux i = let ((acx, acy), cont) = foldl foldAux ((0, 0), 0 :: Integer) ptgs
                               foldAux ((acx, acy), count) ((x, y), i') =
                                   if i == i' then
                                        ((x + acx, y + acy), count + 1)
                                   else ((acx, acy), count)
                           in (acx / (fromIntegral cont), acy / (fromIntegral cont))
               -- Usando streams con chunks
               let res = stParMap (skStrict aux) . stChunk calcMeansChunk . stFromList Z $ [(k - 1), (k - 2) .. 0]
               skRed (arr (\(o, i) -> i : o)) [] -< res

data KMeansStopReason = ByStep | ByThreshold
    deriving Show
            
skKMeans :: Int -> Int -> Skel f ([(Double, Double)], [(Double, Double)], Integer, Double, Integer) ([(Double, Double)], KMeansStopReason)
skKMeans assignChunk calcMeansChunk = proc (ps, ms, k, threshold, step) -> do
    ms' <- skKMeansOneStep assignChunk calcMeansChunk -< (ps, ms, k)
    let epsilon = foldl (\r (m, m') -> max r (sqrt $ dist m m')) 0 (zip ms ms')
    if epsilon < threshold then
        returnA -< (ms', ByThreshold)
    else if step == 0 then
        returnA -< (ms', ByStep)
    else   
        skKMeans assignChunk calcMeansChunk -< (ps, ms', k, threshold, step - 1)

skQuicksortV :: Int -> Skel f (V.Vector Int) (V.Vector Int)
skQuicksortV max = skDaC (skStrict $ V.fromList . sort . V.toList) 
                    (\i -> V.length i < max) 
                    (\xs -> let h = V.head xs
                                t = V.tail xs
                                in [V.filter (< h) t, V.singleton h, V.filter (>= h) t]) 
                    (\_ -> V.concat)

skQuicksortL :: Int -> Skel f [Int] [Int]
skQuicksortL max = skDaC (skStrict sort) 
                    (\i -> length (take max i) < max) 
                    (\xs -> let h = head xs
                                t = tail xs
                            in [filter (< h) t, [h], filter (>= h) t]) 
                    (\_ -> concat)

skFibonacciPrimes :: Skel f Integer [Integer]
skFibonacciPrimes = proc max -> do
    let st = stUntil (arr $ \(acc, (_, b)) -> if b then acc + 1 else acc) 0 (arr (== max)) . 
             stMap skSync .
             stMap (skFork $ \i -> (i, isPrime i)) $
--             stMap (skStrict $ \i -> (i, isPrime i)) $
             stUnfoldr Z fibGen (0 :: Integer, 1 :: Integer)
    skRed (arr (\(o, (i, b)) -> if b then (i:o) else o)) [] -< st

fibGen :: (Integer, Integer) -> (Integer, (Integer, Integer))
fibGen (n0, n1) = (n0, (n1, n0 + n1))

-- Implementacion trucha de isPrime
isPrime :: Integer -> Bool
isPrime n = 
    let q = (truncate :: Double -> Integer) . sqrt . fromIntegral $ n
        d = find (\i -> mod n i == 0) (2 : [3, 5 .. q])
    in (n == 2) || ((n /= 0) && (n /= 1) && (isNothing d))

fibonacciPrimes :: Integer -> [Integer]
fibonacciPrimes max = fibonacciPrimes_ max 0 (0, 1) []

fibonacciPrimes_ :: Integer -> Integer -> (Integer, Integer) -> [Integer] -> [Integer]
fibonacciPrimes_ max count s acc =
    if max == count then
        acc
    else 
        let (f, s') = fibGen s
        in
            if isPrime f then
                fibonacciPrimes_ max (count + 1) s' (f:acc)
            else
                fibonacciPrimes_ max count s' acc

{- =============================================================== -}
{- ======================== Excel Tests ========================== -}
{- =============================================================== -}

defaultIOEC :: IOEC
defaultIOEC = IOEC 1000

execSkForkSimple :: IO ()
execSkForkSimple = do
    print "inicio: execSkForkSimple"
    res <- exec defaultIOEC skForkSimple (1000000000)
    print "fin"
    print res

execSkMapSimple :: IO ()
execSkMapSimple = do
    print "inicio: execSkMapSimple"
    res <- exec defaultIOEC skMapSimple [1000000000, 1000000000, 1000000000, 1000000000]
    print "fin"
    print res

execSkMapChunk :: IO ()
execSkMapChunk = do
    print "inicio: execSkMapChunk"
    res <- exec defaultIOEC skMapChunk (take 500000 $ repeat 5000)
    print "fin"
    print $ length res

execSkMapChunkUnChunk :: IO ()
execSkMapChunkUnChunk = do
    print "inicio: execSkMapChunkUnChunk"
    res <- exec defaultIOEC skMapChunkUnChunk (take 500000 $ repeat 5000)
    print "fin"
    print $ length res

execSkMapChunkUnChunkStop :: IO ()
execSkMapChunkUnChunkStop = do
    print "inicio: execSkMapChunkUnChunkStop"
    res <- exec defaultIOEC skMapChunkUnChunkStop (repeat 5000)
    print "fin"
    print $ length res

execSkMapChunkUnChunkStopIneff :: IO ()
execSkMapChunkUnChunkStopIneff = do
    print "inicio: execSkMapChunkUnChunkStopIneff"
    res <- exec defaultIOEC skMapChunkUnChunkStopIneff (repeat 5000)
    print "fin"
    print $ length res

execSkMapSkelSimple :: IO ()
execSkMapSkelSimple = do
    print "inicio: execSkMapSkelSimple"
    res <- exec defaultIOEC skMapSkelSimple [1000000000, 1000000000, 1000000000, 1000000000]
    print "fin"
    print res

execSkVecProdChunk :: IO ()
execSkVecProdChunk = do
    print "inicio: execSkVecProdChunk"
    res <- exec defaultIOEC skVecProdChunk ([0 .. 100000000], [0 .. 100000000])
    print "fin"
    print res

execSkKMeansOneStep :: IO ()
execSkKMeansOneStep = do
    print "inicio: execSkKMeansOneStep"
    let n = 100
    let k  = 10
    let gen = mkTFGen 1
    let (pxs, pxsRest) = splitAt n $ randomRs (1, 100) gen
    let (pys, pysRest) = splitAt n pxsRest
    let (mxs, mxsRest) = splitAt k pysRest
    let (mys, _) = splitAt k mxsRest
    let ps = zip pxs pys
    let ms = zip mxs mys
    print "ps: "
    print ps
    print "ms: "
    print ms
    resSk <- exec defaultIOEC (skKMeansOneStep 2000 10) (ps, ms, fromIntegral k)
    print "fin"
    print resSk

    print "kMeansTestOneStep"
    print $ kMeansTestOneStep ps ms
    

execSkKMeans :: Int -> Int -> Int -> Int -> Int -> IO ()
execSkKMeans n k chk1 chk2 qSize = do
    print "inicio: execSkKMeans"
    let gen = mkTFGen 1
    let (pxs, pxsRest) = splitAt n $ randomRs (1, 100) gen
    let (pys, pysRest) = splitAt n pxsRest
    let (mxs, mxsRest) = splitAt k pysRest
    let (mys, _) = splitAt k mxsRest
    let ps = zip pxs pys
    let ms = zip mxs mys
    resSk <- exec (IOEC qSize) (skKMeans chk1 chk2) (ps, ms, fromIntegral k, 0.005, 10)
    print "fin"
    print resSk

execKMeansTest :: Int -> Int -> IO ()
execKMeansTest n k = do
    print "inicio: execKMeansTest"
    let gen = mkTFGen 1
    let (pxs, pxsRest) = splitAt n $ randomRs (1, 100) gen
    let (pys, pysRest) = splitAt n pxsRest
    let (mxs, mxsRest) = splitAt k pysRest
    let (mys, _) = splitAt k mxsRest
    let ps :: [(Double, Double)]= zip pxs pys
    let ms = zip mxs mys
    let res = kMeansTest ps ms 0.005 10
--    evaluate $ rnf res
    print "fin"
    print res

execSkQuicksort :: IO ()
execSkQuicksort = do
    let l = take 1000000 . randomRs (0, 1000) $ mkTFGen 7
    print "inicio: execSkQuicksort"
    res <- exec defaultIOEC (skQuicksortL 10000) l
    print "fin"
    print $ length res

execSkFibonacciPrimes :: IO ()
execSkFibonacciPrimes = do
    print "inicio: execSkFibonacciPrimes"
    res <- exec (IOEC 5) skFibonacciPrimes 12
    print "fin"
    print res    

kMeansTest :: (Ord t, Floating t) => [(t, t)] -> [(t, t)] -> t -> Int -> ([(t, t)], KMeansStopReason)
kMeansTest ps ms threshold step = 
    let ms' = kMeansTestOneStep ps ms
        epsilon = foldl (\r (m, m') -> max r (sqrt $ dist m m')) 0 (zip ms ms')
    in
        if epsilon < threshold then
            (ms', ByThreshold)
        else if step == 0 then
            (ms', ByStep)
        else   
            kMeansTest ps ms' threshold (step - 1)

kMeansTestOneStep :: (Ord t, Floating t) => [(t, t)] -> [(t, t)] -> [(t, t)]
kMeansTestOneStep ps ms = 
    let dist (x, y) (x', y')    = (x - x') ** 2 + (y - y') ** 2
        sumPair (x, y) (x', y') = (x + x', y + y')
        divPair (x, y) d        = (x / d, y / d)
        distToMeans p           = map (\m -> (m, dist m p)) ms
        closestMean p           = P.foldl1 (\a b -> if (snd a) < (snd b) then a else b) (distToMeans p)
        pointsWithMean          = map (\p -> (p, closestMean p)) ps
        pointsWithMeanOfMean m  = P.filter (\(_, (m', _)) -> m == m') pointsWithMean
        calcNewMean m           = 
            let pointsWithMeanOfMeanLen = fromIntegral . length . pointsWithMeanOfMean $ m
                sumPointsOfMean = foldl (\p (p', (_, _)) -> sumPair p p') (0, 0) (pointsWithMeanOfMean m)
            in (\p -> divPair p pointsWithMeanOfMeanLen) $ sumPointsOfMean

        calcNewMeans = map calcNewMean ms
    in calcNewMeans

