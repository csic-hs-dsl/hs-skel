{-# LANGUAGE GADTs #-}
{-# LANGUAGE Arrows #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_HADDOCK show-extensions #-}

module Control.Parallel.HsSkel.DSL where

import Data.Traversable --(Traversable)
import Control.Arrow --(Arrow(arr, first, second, (***)), ArrowChoice(left, right, (+++)), ArrowApply(app), returnA)
import Control.Category --(Category, id, (.))
import Control.DeepSeq --(NFData)
import Prelude hiding (id, (.)) --(Bool, Either, Int, Maybe(Just, Nothing), ($), Show, Read, Eq, Ord, (*), Monad)

import Control.Applicative
import Control.Monad
import qualified Control.Monad as M (sequence)

import Control.Concurrent.MVar

-- Por qué el SkPair no puede generar paralelismo?
-- Si se pudiera entonces nada impide que el usuario haga cosas como
--   v1 <- SkStrict c1 -< input
--   returnA -< v1 + 5
-- Eso está mal porque v1 sería un "Future", o al menos una computación que se está ejecutando en paralelo y el usuario 
-- no debería poder manipular eso como un valor cualquiera.

data Future i where
    Now :: i -> Future i
    Bind :: Future i -> (i -> Future o) -> Future o

    Later :: (NFData i) => i -> Future i

instance Functor Future where
    fmap = liftM
 
instance Applicative Future where
    pure  = return
    (<*>) = ap

instance Monad Future where
    return = Now
    (>>=) = Bind

{- instance (Num a) => Num (Future a) where
    (+) f1 f2 = pure (+) <*> f1 <*> f2
    (-) f1 f2 = pure (-) <*> f1 <*> f2
    (*) f1 f2 = pure (*) <*> f1 <*> f2
    negate = (return . negate =<<)
    abs = (return . abs =<<)
    signum = (return . signum =<<)
    fromInteger = Now . fromInteger
-}


data DSL i o where
    Arr    :: (i -> o) -> DSL i o
    Comp   :: DSL x o -> DSL i x -> DSL i o
    Pair   :: DSL i1 o1 -> DSL i2 o2 -> DSL (i1, i2) (o1, o2)

    Par  :: NFData o => (i -> o) -> DSL i (Future o)
    Seq  :: (i -> o) -> DSL i (Future o)



instance Category DSL where
    id  = Arr id
    (.) = Comp

instance Arrow DSL where
    arr = Arr
    first sk = sk `Pair` id
    second sk = id `Pair` sk
    (***) = Pair
    --(&&&) sk1 sk2 = idem to default implementation.



{-
successful :: o -> Future o
successful o = Now o

future :: NFData o => o -> Future o
future o = Later o

sumacostosas1 :: Monad m => (Int -> Int) -> (Int -> Int) -> (Int, Int) -> m (Future Int)
sumacostosas1 c1 c2 (i1, i2) = do
    f1 <- return (future $ c1 i1)
    f2 <- return (future $ c2 i2)
    let fr = do
        v1 <- f1
        v2 <- f2
        successful $ v1 + v2
    return fr

sumacostosas2 :: Monad m => (Int -> Int) -> (Int -> Int) -> (Int, Int) -> m (Future Int)
sumacostosas2 c1 c2 (i1, i2) = do
    let fr = do
        v1 <- future $ c1 i1
        v2 <- future $ c2 i2
        successful $ v1 + v2
    return fr

runPrint :: Future o -> IO o
runPrint (Now o) = putStrLn "<Now/>" >> return o
runPrint (Bind f fun) = do
    putStrLn "<Bind>"
    v <- runPrint f
    o <- runPrint (fun v)
    putStrLn "</Bind>"
    return o
runPrint (Later o) = putStrLn "<Later/>" >> return o

run :: Future o -> IO o
run f = do
    putStrLn "<Future>"
    r <- runPrint f
    putStrLn "</Future>"
    return r

ej1 = do 
    s <- sumacostosas1 (+1) (+2) (1,2)
    run s

-}


successful :: (i -> o) -> DSL i (Future o)
successful f = Seq f

future :: NFData o => (i -> o) -> DSL i (Future o)
future f = Par f

sumacostosas1 :: (Int -> Int) -> (Int -> Int) -> DSL (Int, Int) (Future Int)
sumacostosas1 c1 c2 = proc (i1, i2) -> do
    f1 <- future c1 -< i1
    f2 <- future c2 -< i2
    let fr = do 
            v1 <- f1
            v2 <- f2
            return $ v1 + v2
    id -< fr


sumacostosas1 :: (Int -> Int) -> (Int -> Int) -> DSL (Int, Int) (Future Int)
sumacostosas1 c1 c2 = proc (i1, i2) -> do
    f1 <- future c1 -< i1
    f2 <- future c2 -< i2
    let fr = do 
            v1 <- f1
            v2 <- f2
            return $ v1 + v2
    id -< fr


sumacostosas2 :: (Int -> Int) -> (Int -> Int) -> DSL (Int, Int) (Future Int)
sumacostosas2 c1 c2 = proc (i1, i2) -> do
    let fr = do 
            v1 <- Later (c1 i1)
            v2 <- Later (c2 i2)
            return $ v1 + v2
    id -< fr

run :: (NFData o, Monad m) => i -> DSL i (Future o) -> m o
run i dsl = undefined

