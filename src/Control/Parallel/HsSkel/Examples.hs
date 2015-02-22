{-# LANGUAGE Arrows #-}

module Control.Parallel.HsSkel.Examples (
    execSkParSimple, 
    execSkMapSimple, 
    execSkMapChunk, 
    execSkMapChunkUnChunk,
    execSkMapSkelSimple, 
    execSkVecProdChunk, 
    execSkKMeansOneStep,
    execSkKMeans
) where

import Control.Arrow(returnA, arr)
import Control.Category ((.))
import Control.Parallel.HsSkel
import Control.Parallel.HsSkel.Exec

import Prelude hiding (mapM, id, (.))

import System.Random (randomRs)
import System.Random.TF.Init (mkTFGen)

import Data.List (groupBy, minimumBy, find, sortBy)
import Data.Maybe (fromJust)

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
    skRed (skSeq (\(o, i) -> i:o)) st2 -<< []

-- Este parece andar bien
-- Usa: skPar, skSeq, skSync, stMap, stFromList, stChunk
skMapChunk :: Skel [Integer] [Integer]
skMapChunk = proc l -> do
    let st1 = stMap (skPar $ map doNothing) (stChunk 1000 (stFromList l))
        st2 = stMap skSync st1
    skRed (skSeq (\(o, i) -> i ++ o)) st2 -<< []

-- Este parece andar bien
-- Usa: skPar, skSeq, skSync, stMap, stFromList, stChunk, skUnChunk
-- Ojo que desordena la lista!
skMapChunkUnChunk :: Skel [Integer] [Integer]
skMapChunkUnChunk = proc l -> do
    let st1 = stMap (skPar $ map doNothing) (stChunk 1000 (stFromList l))
        st2 = stMap skSync st1
        st3 = stUnChunk st2
    skRed (skSeq (\(o, i) -> i : o)) st3 -<< []

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
        st1 = stMap (skPar $ sum . map (uncurry (*))) (stChunk 10000000 (stFromList pairs))
        st2 = stMap skSync st1
    skRed (skSeq $ (uncurry (+))) st2 -<< 0



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
                            snd $ foldl1 
                                (\(d, i) (d', i') -> if (d < d') then (d, i) else (d', i')) 
                                (zipWith (\m i -> (dist p m, i)) ms [0 .. ]))
                --ptgsF <- skMap $ skParFromFunc aux -<< ps
                --skMap $ skSync -< ptgsF
                let resChunk = stMap skSync (stMap (skPar $ map aux) (stChunk 1000 (stFromList ps)))
                -- Invierte la lista, pero no es problema
                skRed (arr (\(o, i) -> i : o)) (stUnChunk resChunk) -<< []
                --skRed (skSeq (\(o, i) -> i ++ o)) resChunk -<< []

            -- Sabiendo a que grupo pertenece cada punto, calcula la media de cada grupo. Asume que cada grupo tiene al menos un punto
            calcNewMeans = proc (k, ptgs) -> do
                let aux i = let ((acx, acy), cont) = foldl foldAux ((0, 0), 0) ptgs
                                foldAux ((acx, acy), count) ((x, y), i') = 
                                    if i == i' then 
                                         ((x + acx, y + acy), count + 1) 
                                    else ((acx, acy), count)
                            in (acx / (fromIntegral cont), acy / (fromIntegral cont))
                msF <- skMap $ skPar aux -<< [0 .. k - 1]
                skMap $ skSync -< msF

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
    res <- exec skMapChunk (take 4000 $ repeat 1000000)
    print "fin"
    print res

execSkMapChunkUnChunk :: IO()
execSkMapChunkUnChunk = do
    print "inicio: execSkMapChunkUnChunk"
    res <- exec skMapChunkUnChunk (take 4000 $ repeat 1000000)
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
    print "kMeansTestPablo"
    kMeansTestPablo ps ms >>= print 
    

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
        closestMean p           = foldl1 (\a b -> if (snd a) < (snd b) then a else b) (distToMeans p)
        pointsWithMean          = map (\p -> (p, closestMean p)) ps
        pointsWithMeanOfMean m  = filter (\(_, (m', _)) -> m == m') pointsWithMean
        calcNewMean m           = 
            let pointsWithMeanOfMeanLen = fromIntegral . length . pointsWithMeanOfMean $ m
                sumPointsOfMean = foldl (\p (p', (_, _)) -> sumPair p p') (0, 0) (pointsWithMeanOfMean m)
            in (\p -> divPair p pointsWithMeanOfMeanLen) $ sumPointsOfMean

        calcNewMeans = map calcNewMean ms
    in calcNewMeans



type Point = (Double, Double)
type Mean = (Double, Double)
type Distance = Double
kMeansTestPablo :: [Point] -> [Mean] -> IO [Mean]
kMeansTestPablo points means = do
    let 
        sqr :: Double -> Double
        sqr a = a * a        
        
        fst3 (a, _, _) = a
        snd3 (_, b, _) = b
        trd3 (_, _, c) = c
        
        distance :: Point -> Point -> Distance
        distance (a1, a2) (b1, b2) = sqrt (sqr (a1 - b1) + sqr (a2 - b2))
        
        distances :: [(Point, Mean, Distance)]
        distances = concat $ map (\med -> [(x, med, distance x med) | x <- points]) means
    
        grouped :: [[(Point, Mean, Distance)]]
        grouped = groupBy (\p1 p2 -> fst3 p1 == fst3 p2) $ sortBy (\p1 p2 -> compare (fst3 p1) (fst3 p2)) distances
        
        mins :: [(Point, Mean, Distance)]
        mins = map (minimumBy (\p1 p2 -> compare (trd3 p1) (trd3 p2))) grouped
        
        groupedMeds :: [[(Point, Mean, Distance)]]
        groupedMeds = groupBy (\p1 p2-> snd3 p1 == snd3 p2) $ sortBy (\p1 p2 -> compare (snd3 p1) (snd3 p2)) mins
        
        meds :: [(Mean, [Point])]
        meds = map (\ls -> (snd3 $ head ls, map fst3 ls)) groupedMeds
        
        sumPair (a1, a2) (b1, b2) = (a1 + b1, a2 + b2)
        dividePair (a1, a2) (b1, b2) = (a1 / b1, a2 / b2)
        pair a = (a, a)
        
        newMeds :: [(Mean, Mean)]
        newMeds = map (\(m, ls) -> (m, dividePair (foldl1 sumPair ls) (pair $ fromIntegral $ length ls))) meds

        newMedsSorted :: [Mean]
        newMedsSorted = map (\m -> snd $ fromJust $ find (\(m2, _) -> m == m2) newMeds) means
    return newMedsSorted
