{-# LANGUAGE ScopedTypeVariables #-}

module Main (
    main
    )
where

import Control.Arrow (arr)
import Control.Category ((.))
import Control.DeepSeq (NFData)
import Control.Monad.State (StateT, evalStateT, get, modify)
import Control.Monad.Trans (liftIO)
import Control.Parallel.HsSkel
import Control.Parallel.HsSkel.Exec (exec)

import Data.Vector (toList)

import System.IO.Unsafe (unsafePerformIO)

import Test.QuickCheck (Arbitrary(arbitrary), CoArbitrary(), Property, Testable, quickCheckResult)
import Test.QuickCheck.Gen (Gen, choose, vectorOf)
import Test.QuickCheck.Test (Result(..), isSuccess)
import Test.QuickCheck.Monadic (assert, monadicIO, run)

import Prelude hiding ((.))


{-- ================================================================ --}
{-- ================================================================ --}
data FunTest a b = FunTest (a -> b) [a]

instance (Show a, Show b) => Show (FunTest a b) where
    show (FunTest f is) = "FunTest [" ++ showFunction f is ++ "]"
        where 
            showFunction _ [] = ""
            showFunction f [x] = showValue f x
            showFunction f (x:xs) = showValue f x ++ ", " ++ showFunction f xs
            showValue f x = show x ++ "->" ++ show (f x)

instance (Arbitrary a, CoArbitrary a, Arbitrary b) => Arbitrary (FunTest a b) where
    arbitrary = do
        inputs <- vectorOf 100 arbitrary
        fun <- arbitrary :: Gen (a -> b)
        return $ FunTest fun inputs


instance (NFData o, Arbitrary o) => Arbitrary (Stream o) where
    arbitrary = do
        list <- arbitrary
        return $ stFromList list

instance Show a => Show (Stream a) where
    show st = show $ unsafePerformIO $ streamToList st


data StChunkTestData o = StChunkTestData Int (Stream o) deriving Show

instance (NFData o, Arbitrary o) => Arbitrary (StChunkTestData o) where
    arbitrary = do
        size <- choose(1, 5)
        stream <- arbitrary
        return $ StChunkTestData size stream


chunksOf :: Int -> [a] -> [[a]]
chunksOf _ [] = []
chunksOf n xs 
    | n <= 0    = error "chunksOf: the chunk size must be greater than zero."
    | otherwise = take n xs : chunksOf n (drop n xs)

propOnIO :: IO Bool -> Property
propOnIO code = monadicIO $ assert =<< run code

streamToList :: Stream o -> IO [o]
streamToList stream = exec (skRed (arr $ \(o, i) -> i:o) stream) [] >>= return . reverse


type StateWithIO s a = StateT s IO a
type ResultMonad a  = StateWithIO [(String, Result)] a
type Results = ResultMonad ()

appendResult :: String -> Result -> ResultMonad ()
appendResult n r = modify (\s -> s ++ [(n, r)])

testWith :: Testable prop => (String, t) -> [(String, t -> prop)] -> Results
testWith (name, fun) props = do 
    liftIO $ putStr "Testing: " >> putStrLn name
    let 
        runTest (testName, prop) = do
            putStr "+" >> putStrLn testName
            result <- quickCheckResult (prop fun)
            return (testName ++ "(" ++ name ++ ")", result)
    results <- liftIO $ (mapM runTest props)
    mapM_ (uncurry appendResult) results

{-- ================================================================ --}
{-- ================================================================ --}

propExecSkelVsArbFunIsOk :: (Arbitrary i, CoArbitrary i, Arbitrary o, Eq o) => ((i -> o) -> Skel i o) -> FunTest i o -> Property
propExecSkelVsArbFunIsOk skel (FunTest f is) = propOnIO $ do
    res1 <- mapM (exec (skel f)) is
    let res2 = map f is
    return (res1 == res2)

propExecSkelVsFunIsOk :: (Eq o) => ((i -> o) -> Skel i o) -> (i -> o) -> i -> Property
propExecSkelVsFunIsOk skel f i = propOnIO $ do
    res1 <- exec (skel f) i
    let res2 = f i
    return (res1 == res2)

propExecArrIsOk :: (Eq o) => (i -> o) -> i -> Property
propExecArrIsOk = propExecSkelVsFunIsOk arr

propExecSkSeqIsOk :: (NFData o, Eq o) => (i -> o) -> i -> Property
propExecSkSeqIsOk = propExecSkelVsFunIsOk skSeq

propExecSkSynkCompSkParIsOk :: (NFData o, Eq o) => (i -> o) -> i -> Property
propExecSkSynkCompSkParIsOk = propExecSkelVsFunIsOk (\f -> skSync . skPar f)

propExecSkSynkCompSkParCompSkSeqIsOk :: (NFData o, Eq o) => (i -> o) -> i -> Property
propExecSkSynkCompSkParCompSkSeqIsOk = propExecSkelVsFunIsOk (\f -> skSync . skPar (skSeq f))

propStreamToListIsOk :: (Eq i, NFData i) => [i] -> Property
propStreamToListIsOk list = propOnIO $ do
    let stream = stFromList list
    res <- streamToList stream
    return (list == res)

propExecStMapIsOk :: (NFData o, Eq o) => (i -> o) -> Stream i -> Property
propExecStMapIsOk f stream = propOnIO $ do
    list <- streamToList stream
    res1 <- streamToList $ stMap (skSeq f) stream
    let expected = map f list
    return (res1 == expected)

propExecStChunkIsOk :: (Eq i, Arbitrary i) => StChunkTestData i -> Property
propExecStChunkIsOk (StChunkTestData size stream) = propOnIO $ do
    list <- streamToList stream
    res1 <- streamToList $ stChunk size stream
    let result = map toList res1
        expected = chunksOf size list
    return (expected == result)

propExecStUnchunkIsOk :: (Eq i, Arbitrary i) => StChunkTestData i -> Property
propExecStUnchunkIsOk (StChunkTestData size stream) = propOnIO $ do
    expected <- streamToList stream
    result <- streamToList $ stUnChunk $ stChunk size stream
    return (expected == result)

{-- ================================================================ --}
{-- ================================================================ --}

testExecSkelVsArbFunIsOk :: (String, (Int -> Int) -> Skel Int Int) -> Results
testExecSkelVsArbFunIsOk (name, skel) = do
    liftIO $ putStrLn ("testExecSkelVsArbFunIsOk: " ++ name)
    appendResult name =<< (liftIO $ quickCheckResult $ (propExecSkelVsArbFunIsOk skel :: FunTest Int Int -> Property))
    
testWithReverse :: Results
testWithReverse = testWith ("reverse", reverse :: [Int] -> [Int]) [
        ("propExecArrIsOk", propExecArrIsOk),
        ("propExecSkSeqIsOk", propExecSkSeqIsOk),
        ("propExecSkSynkCompSkParIsOk", propExecSkSynkCompSkParIsOk),
        ("propExecSkSynkCompSkParCompSkSeqIsOk", propExecSkSynkCompSkParCompSkSeqIsOk)
    ]

testWithPlus2 :: Results
testWithPlus2 = testWith ("(+2)", ((+ 2) :: Int -> Int)) [
        ("propExecArrIsOk", propExecArrIsOk),
        ("propExecSkSeqIsOk", propExecSkSeqIsOk),
        ("propExecSkSynkCompSkParIsOk", propExecSkSynkCompSkParIsOk),
        ("propExecSkSynkCompSkParCompSkSeqIsOk", propExecSkSynkCompSkParCompSkSeqIsOk)
    ]

testExecSkRedIsOk :: Results
testExecSkRedIsOk = do
    let name = "testExecSkRedIsOk"
    liftIO $ putStrLn name
    appendResult name =<< (liftIO $ quickCheckResult $ (propStreamToListIsOk :: [Int] -> Property))

testExecStMapIsOk :: Results
testExecStMapIsOk = do
    let name = "testExecStMapIsOk"
    liftIO $ putStrLn name
    appendResult name =<< (liftIO $ quickCheckResult $ propExecStMapIsOk (*(2 :: Int)))

testExecStChunkIsOk :: Results
testExecStChunkIsOk = do
    let name = "testExecStChunkIsOk"
    liftIO $ putStrLn name
    appendResult name =<< (liftIO $ quickCheckResult $ (propExecStChunkIsOk :: StChunkTestData Int -> Property))

testExecStUnchunkIsOk :: Results
testExecStUnchunkIsOk = do
    let name = "testExecStUnchunkIsOk"
    liftIO $ putStrLn name
    appendResult name =<< (liftIO $ quickCheckResult $ (propExecStUnchunkIsOk :: StChunkTestData Int -> Property))


{-- ================================================================ --}
{-- ================================================================ --}

execAllTests :: IO ()
execAllTests = do
    let 
        allTests = do
            testWithReverse
            testWithPlus2
            testExecSkelVsArbFunIsOk ("arr", arr)
            testExecSkelVsArbFunIsOk ("skSeq", skSeq)
            testExecSkelVsArbFunIsOk ("skSync . skPar", \f -> skSync . skPar f)
            testExecSkelVsArbFunIsOk ("skSync . skPar skSeq", \f -> skSync . skPar (skSeq f))
            testExecSkelVsArbFunIsOk ("skSync . skPar arr", \f -> skSync . skPar (arr f :: Skel Int Int))
            testExecSkelVsArbFunIsOk ("skSync . skPar arr", \f -> skSync . skPar (arr f :: Skel Int Int))
            testExecSkRedIsOk
            testExecStMapIsOk
            testExecStChunkIsOk
            testExecStUnchunkIsOk
    results <- evalStateT (allTests >> get) []
    let errors = filter (not . isSuccess . snd) results
        cntErr = length errors
    print errors
    if cntErr > 0 
        then
            fail $ "There was " ++ show cntErr ++ " errors!"
        else
            return ()

main :: IO ()
main = execAllTests