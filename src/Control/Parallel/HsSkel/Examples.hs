{-# LANGUAGE Arrows #-}

module Control.Parallel.HsSkel.Examples (
    execSkParSimple, 
    execSkMapSimple, 
    execSkMapChunk, 
    execSkMapChunkUnChunk,
    execSkMapChunkUnChunkStop,
    execSkMapChunkUnChunkStopIneff,
    execSkMapSkelSimple, 
    execSkVecProdChunk, 
    execSkKMeansOneStep,
    execSkKMeans
) where

import Control.Arrow(returnA, arr)
import Control.Category ((.))
import Control.Parallel.HsSkel
import Control.Parallel.HsSkel.Exec

import Data.Functor (fmap)
import Data.Vector (Vector, concat, foldl1)

import Prelude hiding ((.), concat, id, mapM, fmap, foldl1)
import qualified Prelude as P

import System.Random (randomRs)
import System.Random.TF.Init (mkTFGen)

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
-- Usa: skPar, skSeq, skSync
skParSimple :: Skel Integer (Integer, Integer, Integer, Integer)
skParSimple = proc a -> do
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
-- Usa: skPar, skSeq, skSync, stMap, stFromList
skMapSimple :: Skel [Integer] [Integer]
skMapSimple = proc l -> do
    let st1 = stMap (skPar doNothing) (stFromList l)
        st2 = stMap skSync st1
    skRed (arr (\(o, i) -> i : o)) st2 -<< []

-- Este parece andar bien
-- Usa: skPar, skSeq, skSync, stMap, stFromList, stChunk
skMapChunk :: Skel [Integer] (Vector Integer)
skMapChunk = proc l -> do
    let st1 = stMap (skPar $ fmap doNothing) (stChunk 1000 (stFromList l))
        st2 = stMap skSync st1
    vecs <- skRed (skSeq (\(o, i) -> i : o)) st2 -<< []
    skSeq concat -< vecs

-- Este parece andar bien
-- Usa: skPar, skSeq, skSync, stMap, stFromList, stChunk, skUnChunk
-- Ojo que desordena la lista!
skMapChunkUnChunk :: Skel [Integer] [Integer]
skMapChunkUnChunk = proc l -> do
    let st = stUnChunk . stMap skSync . stMap (skPar $ fmap doNothing) . stChunk 1000 . stFromList $ l
    skRed (arr (\(o, i) -> i : o)) st -<< []
    
-- Este parece andar bien
-- Usa: skPar, skSeq, skSync, stMap, stFromList, stChunk, skUnChunk, stStop
-- Ojo que desordena la lista!
skMapChunkUnChunkStop :: Skel [Integer] [Integer]
skMapChunkUnChunkStop = proc l -> do
    let st = stUnChunk . stMap skSync . stMap (skPar $ fmap doNothing) . stChunk 1000 . stStop (\acc _ -> acc + 1 :: Integer) 0 (== 500000) . stGen listGen $ l
    skRed (arr (\(o, i) -> i : o)) st -<< []
    where
        listGen :: [Integer] -> (Integer, [Integer])
        listGen (a:as) = (a, as)
        listGen _ = undefined -- La lista debe ser infinita

-- Este parece andar bien
-- Usa: skPar, skSeq, skSync, stMap, stFromList, stChunk, skUnChunk, stStop
-- Ojo que desordena la lista!
-- Hay que tener en cuanta que esta es una solucion ineficiente, ya que el Stop está al final, es sólo para probar su propagación
skMapChunkUnChunkStopIneff :: Skel [Integer] [Integer]
skMapChunkUnChunkStopIneff = proc l -> do
    let st = stStop (\acc _ -> acc + 1 :: Integer) 0 (== 500000) . stUnChunk . stMap skSync . stMap (skPar $ fmap doNothing) . stChunk 1000 . stGen listGen $ l
    skRed (arr (\(o, i) -> i : o)) st -<< []
    where
        listGen :: [Integer] -> (Integer, [Integer])
        listGen (a:as) = (a, as)
        listGen _ = undefined -- La lista debe ser infinita

-- Este parece andar bien
-- Usa: skPar, skSeq, skSync, skMap, skTraverseF
skMapSkelSimple :: Skel [Integer] [Integer]
skMapSkelSimple = proc l -> do 
    lf <- skMap (skPar doNothing) -< l
    skSync . skTraverseF -< lf

-- Este no anda bien
skVecProdChunk :: Skel ([Double], [Double]) Double
skVecProdChunk = proc (vA, vB) -> do
    let pairs = zip vA vB -- lazy
        st = stMap skSync . stMap (skPar $ foldl1 (+) . fmap (uncurry (*))) . stChunk 10000000 . stFromList $ pairs
    skRed (skSeq $ (uncurry (+))) st -<< 0

dist :: (Floating a) => (a, a) -> (a, a) -> a
dist (x, y) (x', y') = (x - x') ** 2 + (y - y') ** 2

skKMeansOneStep :: Skel (([(Double, Double)], [(Double, Double)]), Integer) [(Double, Double)]
skKMeansOneStep = proc ((ps, ms), k) -> do
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

               let res = stUnChunk . stMap skSync . stMap (skPar $ fmap aux) . stChunk 2000 . stFromList $ ps

               -- Invierte la lista, pero no es problema
               skRed (arr (\(o, i) -> i : o)) res -<< []

            -- Sabiendo a que grupo pertenece cada punto, calcula la media de cada grupo. Asume que cada grupo tiene al menos un punto
            calcNewMeans = proc (k, ptgs) -> do
               let aux i = let ((acx, acy), cont) = foldl foldAux ((0, 0), 0 :: Integer) ptgs
                               foldAux ((acx, acy), count) ((x, y), i') =
                                   if i == i' then
                                        ((x + acx, y + acy), count + 1)
                                   else ((acx, acy), count)
                           in (acx / (fromIntegral cont), acy / (fromIntegral cont))
               -- Usando streams con chunks
               let res = stUnChunk . stMap skSync . stMap (skPar $ fmap aux) . stChunk 10 . stFromList $ [(k - 1), (k - 2) .. 0]
               skRed (arr (\(o, i) -> i : o)) res -<< []

data KMeansStopReason = ByStep | ByThreshold
    deriving Show
            
skKMeans :: Skel (([(Double, Double)], [(Double, Double)]), Integer, Double, Integer) ([(Double, Double)], KMeansStopReason)
skKMeans = proc ((ps, ms), k, threshold, step) -> do
    ms' <- skKMeansOneStep -< ((ps, ms), k)
    let epsilon = trace ("step: " ++ show step) $ foldl (\r (m, m') -> max r (sqrt $ dist m m')) 0 (zip ms ms')
    if epsilon < threshold then
        returnA -< (ms', ByThreshold)
    else if step == 0 then
        returnA -< (ms', ByStep)
    else   
        skKMeans -< ((ps, ms'), k, threshold, step - 1)


{- =============================================================== -}
{- ======================== Excel Tests ========================== -}
{- =============================================================== -}

execSkParSimple :: IO()
execSkParSimple = do
    print "inicio: execSkParSimple"
    res <- exec skParSimple (1000000000)
    print "fin"
    print res

execSkMapSimple :: IO()
execSkMapSimple = do
    print "inicio: execSkMapSimple"
    res <- exec skMapSimple [1000000000, 1000000000, 1000000000, 1000000000]
    print "fin"
    print res

execSkMapChunk :: IO()
execSkMapChunk = do
    print "inicio: execSkMapChunk"
    res <- exec skMapChunk (take 500000 $ repeat 5000)
    print "fin"
    print res

execSkMapChunkUnChunk :: IO()
execSkMapChunkUnChunk = do
    print "inicio: execSkMapChunkUnChunk"
    res <- exec skMapChunkUnChunk (take 500000 $ repeat 5000)
    print "fin"
    print res

execSkMapChunkUnChunkStop :: IO()
execSkMapChunkUnChunkStop = do
    print "inicio: execSkMapChunkUnChunkStop"
    res <- exec skMapChunkUnChunkStop (repeat 5000)
    print "fin"
    print res

execSkMapChunkUnChunkStopIneff :: IO()
execSkMapChunkUnChunkStopIneff = do
    print "inicio: execSkMapChunkUnChunkStopIneff"
    res <- exec skMapChunkUnChunkStopIneff (repeat 5000)
    print "fin"
    print res

execSkMapSkelSimple :: IO()
execSkMapSkelSimple = do
    print "inicio: execSkMapSkelSimple"
    res <- exec skMapSkelSimple [1000000000, 1000000000, 1000000000, 1000000000]
    print "fin"
    print res

execSkVecProdChunk :: IO()
execSkVecProdChunk = do
    print "inicio: execSkVecProdChunk"
    res <- exec skVecProdChunk ([0 .. 100000000], [0 .. 100000000])
    print "fin"
    print res

execSkKMeansOneStep :: IO()
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
    resSk <- exec skKMeansOneStep ((ps, ms), fromIntegral k)
    print "fin"
    print resSk

    print "kMeansTestMauro"
    print $ kMeansTestMauro ps ms
    

execSkKMeans :: IO()
execSkKMeans = do
    print "inicio: execSkKMeans"
    let n = 200000
    let k  = 100
    let gen = mkTFGen 1
    let (pxs, pxsRest) = splitAt n $ randomRs (1, 100) gen
    let (pys, pysRest) = splitAt n pxsRest
    let (mxs, mxsRest) = splitAt k pysRest
    let (mys, _) = splitAt k mxsRest
    let ps = zip pxs pys
    let ms = zip mxs mys
    --print "ps: "
    --print ps
    --print "ms: "
    --print ms
    resSk <- exec skKMeans ((ps, ms), fromIntegral k, 0.005, 10)
    print "fin"
    print resSk





kMeansTestMauro :: (Ord t, Floating t) => [(t, t)] -> [(t, t)] -> [(t, t)]
kMeansTestMauro ps ms = 
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

