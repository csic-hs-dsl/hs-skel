{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}

module Main (
    main
) where

import Control.Arrow (arr)
import Control.Category ((.))
import Control.DeepSeq (NFData)
import Control.Monad.State (StateT, evalStateT, get, modify)
import Control.Monad.Trans (liftIO)
import Control.Parallel.HsSkel
import Control.Parallel.HsSkel.Exec.Default

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


instance (NFData o, Arbitrary o) => Arbitrary (Stream Z IOFuture o) where
    arbitrary = do
        list <- arbitrary
        return $ stFromList list

instance Show a => Show (Stream Z IOFuture a) where
    show st = show $ unsafePerformIO $ streamToList st


data StChunkTestData o = StChunkTestData Int (Stream Z IOFuture o) deriving Show

instance (NFData o, Arbitrary o) => Arbitrary (StChunkTestData o) where
    arbitrary = do
        size <- choose(1, 5)
        stream <- arbitrary
        return $ StChunkTestData size stream


propOnIO :: IO Bool -> Property
propOnIO code = monadicIO $ assert =<< run code

streamToList :: (DIM dim) => Stream dim IOFuture o -> IO [o]
streamToList stream = exec defaultIOEC (skRed (arr $ \(o, i) -> i:o) []) stream >>= return . reverse


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
defaultIOEC :: IOEC
defaultIOEC = IOEC 1000

propExecSkelVsArbFunIsOk :: (Arbitrary i, CoArbitrary i, Arbitrary o, Eq o) => ((i -> o) -> Skel IOFuture i o) -> FunTest i o -> Property
propExecSkelVsArbFunIsOk skel (FunTest f is) = propOnIO $ do
    res1 <- mapM (exec defaultIOEC (skel f)) is
    let res2 = map f is
    return (res1 == res2)

propExecSkelVsFunIsOk :: (Eq o) => ((i -> o) -> Skel IOFuture i o) -> (i -> o) -> i -> Property
propExecSkelVsFunIsOk skel f i = propOnIO $ do
    res1 <- exec defaultIOEC (skel f) i
    let res2 = f i
    return (res1 == res2)

propExecArrIsOk :: (Eq o) => (i -> o) -> i -> Property
propExecArrIsOk = propExecSkelVsFunIsOk arr

propExecSkSeqIsOk :: (NFData o, Eq o) => (i -> o) -> i -> Property
propExecSkSeqIsOk = propExecSkelVsFunIsOk skStrict

propExecSkSynkCompSkForkIsOk :: (NFData o, Eq o) => (i -> o) -> i -> Property
propExecSkSynkCompSkForkIsOk = propExecSkelVsFunIsOk (\f -> skSync . skFork f)

propStreamToListIsOk :: (Eq i, NFData i) => [i] -> Property
propStreamToListIsOk list = propOnIO $ do
    let stream = stFromList list
    res <- streamToList stream
    return (list == res)

propExecStMapIsOk :: (NFData o, Eq o) => (i -> o) -> Stream Z IOFuture i -> Property
propExecStMapIsOk f stream = propOnIO $ do
    list <- streamToList stream
    res1 <- streamToList $ stMap (skStrict f) stream
    let expected = map f list
    return (res1 == expected)

propExecStChunkIsOk :: (Eq i, Arbitrary i) => StChunkTestData i -> Property
propExecStChunkIsOk (StChunkTestData size stream) = propOnIO $ do
    expected <- streamToList stream
    result <- streamToList $ stChunk size stream
    return (expected == result)

propExecStUnchunkIsOk :: (Eq i, Arbitrary i) => StChunkTestData i -> Property
propExecStUnchunkIsOk (StChunkTestData size stream) = propOnIO $ do
    expected <- streamToList stream
    result <- streamToList $ stUnChunk $ stChunk size stream
    return (expected == result)

{-- ================================================================ --}
{-- ================================================================ --}

testExecSkelVsArbFunIsOk :: (String, (Int -> Int) -> Skel IOFuture Int Int) -> Results
testExecSkelVsArbFunIsOk (name, skel) = do
    liftIO $ putStrLn ("testExecSkelVsArbFunIsOk: " ++ name)
    appendResult name =<< (liftIO $ quickCheckResult $ (propExecSkelVsArbFunIsOk skel :: FunTest Int Int -> Property))
    
testWithReverse :: Results
testWithReverse = testWith ("reverse", reverse :: [Int] -> [Int]) [
        ("propExecArrIsOk", propExecArrIsOk),
        ("propExecSkSeqIsOk", propExecSkSeqIsOk),
        ("propExecSkSynkCompSkForkIsOk", propExecSkSynkCompSkForkIsOk)
    ]

testWithPlus2 :: Results
testWithPlus2 = testWith ("(+2)", ((+ 2) :: Int -> Int)) [
        ("propExecArrIsOk", propExecArrIsOk),
        ("propExecSkSeqIsOk", propExecSkSeqIsOk),
        ("propExecSkSynkCompSkForkIsOk", propExecSkSynkCompSkForkIsOk)
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
            testExecSkelVsArbFunIsOk ("skStrict", skStrict)
            testExecSkelVsArbFunIsOk ("skSync . skFork", \f -> skSync . skFork f)
            testExecSkelVsArbFunIsOk ("skSync . skFork arr", \f -> skSync . skFork (arr f :: Skel IOFuture Int Int))
            testExecSkelVsArbFunIsOk ("skSync . skFork arr", \f -> skSync . skFork (arr f :: Skel IOFuture Int Int))
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
