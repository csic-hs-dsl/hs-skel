{-# LANGUAGE Arrows #-}

module Control.Parallel.HsSkel.Examples (execSkParSimple, execSkMapSimple, execSkMapChunk, execSkMapSkelSimple, execSkVecProdChunk, execSkKMeans)
where

import Control.Arrow(returnA)
import Control.Category ((.))
import Control.Parallel.HsSkel
import Control.Parallel.HsSkel.Exec

import Prelude hiding (mapM, id, (.))

import System.Random (randomRs)
import System.Random.TF.Init (mkTFGen)

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
    let st1 = stMap (stFromList l) (skParFromFunc doNothing)
        st2 = stMap st1 skSync
    skRed st2 (skSeq (\(o, i) -> i:o)) -<< []

-- Este parece andar bien
-- Usa: skPar, skSeq, skSync, stMap, stFromList, stChunk
skMapChunk :: Skel [Integer] [Integer]
skMapChunk = proc l -> do
    let st1 = stMap (stChunk (stFromList l) 1000) (skParFromFunc $ map doNothing)
        st2 = stMap st1 skSync
    skRed st2 (skSeq (\(o, i) -> i ++ o)) -<< []

-- Este parece andar bien
-- Usa: skPar, skSeq, skSync, skMap, skTraverseF
skMapSkelSimple :: Skel [Integer] [Integer]
skMapSkelSimple = proc l -> do 
    lf <- skMap (skParFromFunc doNothing) -< l
    skSync . skTraverseF -< lf

-- Este no anda bien
skVecProdChunk :: Skel ([Double], [Double]) Double
skVecProdChunk = proc (vA, vB) -> do
    let pairs = zip vA vB -- lazy
        st1 = stMap (stChunk (stFromList pairs) 10000000) (skParFromFunc $ sum . map (uncurry (*)))
        st2 = stMap st1 skSync
    skRed st2 (skSeq $ (uncurry (+))) -<< 0

-- TODO: por ahora solo hace un paso
skKMeans :: Skel (([(Double, Double)], [(Double, Double)]), Integer, Double) [(Double, Double)]
skKMeans = proc ((ps, ms), k, threshold) -> do
    ptgs <- calcPointGroup -< (ps, ms)
    ms' <- calcNewMeans -< (k, ptgs)
    let epsilon = foldl (\r (m, m') -> max r (sqrt $ dist m m')) 0 (zip ms ms')
    if epsilon < threshold then
        returnA -< ms'
    else    
        skKMeans -< ((ps, ms'), k, threshold)

        where
            -- A partir de los puntos y las medias calcula a que grupo (índice de ms) pertenece cada punto
            calcPointGroup = proc (ps, ms) -> do
                let aux p = (p, 
                            snd $ foldl1 
                                (\(d, i) (d', i') -> if (d < d') then (d, i) else (d', i')) 
                                (zipWith (\m i -> (dist p m, i)) ms [0 .. ]))
                ptgsF <- skMap $ skParFromFunc aux -<< ps
                skMap $ skSync -< ptgsF

            -- Sabiendo a que grupo pertenece cada punto, calcula la media de cada grupo. Asume que cada grupo tiene al menos un punto
            calcNewMeans = proc (k, ptgs) -> do
                msF <- skMap $ skParFromFunc (\i -> let ((acx, acy), cont) = foldl1 foldAux ptgs
                                                        foldAux ((acx, acy), count) ((x, y), i') = if i == i' then 
                                                                                                        ((x + acx, y + acy), count + 1) 
                                                                                                        else ((acx, acy), count)
                                                    in (acx / (fromIntegral cont), acy / (fromIntegral cont))) -<< [0 .. k - 1]
                skMap $ skSync -< msF
            dist (x, y) (x', y') = (x - x') ** 2 + (y - y') ** 2
            
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

execSkKMeans :: IO()
execSkKMeans = do
    print "inicio: execSkKMeans"
    let n = 100
    let k  = 10
    let gen = mkTFGen 1 -- mkStdGen 1
    let (pxs, pxsRest) = splitAt n $ randomRs (1, 100) gen
    let (pys, pysRest) = splitAt n pxsRest
    let (mxs, mxsRest) = splitAt k pysRest
    let (mys, _) = splitAt k mxsRest
    let ps = zip pxs pys
    let ms = zip mxs mys
    print ps
    print ms
    res <- exec skKMeans ((ps, ms), fromIntegral k, 0.5)
    print "fin"
    print res
