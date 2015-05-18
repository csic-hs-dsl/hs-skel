{-# LANGUAGE GADTs #-}
{-# LANGUAGE Arrows #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_HADDOCK show-extensions #-}

-- | This module defines the DSL to construct Streams and Skeletons.
module Control.Parallel.HsSkel.DSL (
    -- * Types
    Skel(..),
    -- * Execution
    Exec(Context, exec),
) where

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
    Later :: Future i -> (i -> Future o) -> Future o


instance Functor Future where
    fmap = liftM
 
instance Applicative Future where
    pure  = return
    (<*>) = ap

instance Monad Future where
    return = Now
    (>>=) = Later

instance (Num a) => Num (Future a) where
    (+) f1 f2 = pure (+) <*> f1 <*> f2
    (-) f1 f2 = pure (-) <*> f1 <*> f2
    (*) f1 f2 = pure (*) <*> f1 <*> f2
    negate = (return . negate =<<)
    abs = (return . abs =<<)
    signum = (return . signum =<<)
    fromInteger = Now . fromInteger

data Skel i o where
    SkLazy   :: (i -> o) -> Skel i o
    SkComp   :: Skel x o -> Skel i x -> Skel i o
    SkPair   :: Skel i1 o1 -> Skel i2 o2 -> Skel (i1, i2) (o1, o2)
    SkChoice :: Skel i o -> Skel i' o' -> Skel (Either i i') (Either o o')
    SkApply  :: Skel (Skel i o, i) o

    SkStrict   :: (NFData o) => (i -> o) -> Skel i (Future o)
    SkAndThen  :: (NFData o) => (i -> o) -> Skel (Future i) (Future o)
    SkTuple    :: (NFData i1, NFData i2) => Skel (Future i1, Future i2) (Future (i1, i2))
    SkTraverse :: (NFData i, Traversable t) => Skel (t (Future i)) (Future (t i))


instance Category (Skel) where
    id  = SkLazy id
    (.) = SkComp

instance Arrow (Skel) where
    arr = SkLazy
    first sk = sk `SkPair` id
    second sk = id `SkPair` sk
    (***) = SkPair
    --(&&&) sk1 sk2 = idem to default implementation.

instance ArrowChoice (Skel) where
    left sk = SkChoice sk id
    right sk = SkChoice id sk
    (+++) = SkChoice
    --(|||) sk1 sk2 = idem to default implementation.

instance ArrowApply (Skel) where
    app = SkApply


instance Show (Future i) where
    show (Now _) = "Now"
    show (Later f _) = "Later (" ++ show f ++ ")"

instance Show (Skel a b) where
    show (SkLazy _) = "<SkLazy/>"
    show (SkComp skel1 skel2) = "<SkComp> " ++ show skel1 ++ " " ++ show skel2 ++ "</SkComp>"
    show (SkPair skel1 skel2) = "<SkPair> " ++ show skel1 ++ " " ++ show skel2 ++ "</SkPair>"
    show (SkChoice skel1 skel2) = "<SkChoice> " ++ show skel1 ++ " " ++ show skel2 ++ "</SkChoice>"
    show (SkApply) = "<SkApply/>"
    show (SkStrict _) = "<SkStrict/>"
    show (SkAndThen _) = "<SkAndThen/>"
    show (SkTuple) = "<SkTuple/>"
    show (SkTraverse) = "<SkTraverse/>"

{- ================================================================== -}
{- ======================= Execution Context ======================== -}
{- ================================================================== -}

class (Monad m) => Exec m where
    type Context m :: *
    exec :: Context m -> Skel i o -> i -> m o



data IOContext = IOContext
instance Exec IO where
    type Context IO = IOContext
    exec = execIO

execIO :: IOContext -> Skel i o -> i -> IO o
execIO ctx skel input = undefined


sumacostosas :: (Int -> Int) -> (Int -> Int) -> Skel (Int, Int) (Future Int)
sumacostosas c1 c2 = proc (i1, i2) -> do
    f1 <- SkStrict c1 -< i1
    f2 <- SkStrict c2 -< i2
    ft <- SkTuple -< (f1, f2)
    SkAndThen (uncurry (+)) -< ft

sumacostosas2 :: (Int -> Int) -> (Int -> Int) -> (Int -> Int) -> Skel (Int, Int, Int) (Future Int)
sumacostosas2 c1 c2 c3 = proc (i1, i2, i3) -> do
    f1 <- SkStrict c1 -< i1
    f2 <- SkStrict c2 -< i2
    f3 <- SkStrict c3 -< i3
    fl <- SkTraverse -< [f1, f2, f3]
    SkAndThen sum -< fl


-- Esto es practicamente Scala
-- sumacostosas usando la mónada Future
sumacostosas3 :: (Int -> Int) -> (Int -> Int) -> Skel (Int, Int) (Future Int)
sumacostosas3 c1 c2 = proc (i1, i2) -> do
    f1 <- SkStrict c1 -< i1
    f2 <- SkStrict c2 -< i2
    let fr = do
        v1 <- f1
        v2 <- f2
        return $ v1 + v2
    returnA -< fr

-- sumacostosas usando la mónada Future con fmap, uncurry y fTuple
sumacostosas4 :: (Int -> Int) -> (Int -> Int) -> Skel (Int, Int) (Future Int)
sumacostosas4 c1 c2 = proc (i1, i2) -> do
    f1 <- SkStrict c1 -< i1
    f2 <- SkStrict c2 -< i2
    returnA -< pure (+) <*> f1 <*> f2

-- sumacostosas usando la mónada Future con sequence
sumacostosas5 :: (Int -> Int) -> (Int -> Int) -> Skel (Int, Int) (Future Int)
sumacostosas5 c1 c2 = proc (i1, i2) -> do
    f1 <- SkStrict c1 -< i1
    f2 <- SkStrict c2 -< i2
    returnA -< do
        fr <- M.sequence $ [f1, f2]
        return $ sum fr

-- sumacostosas usando la mónada Future y su instancia de Num
sumacostosas6 :: (Int -> Int) -> (Int -> Int) -> Skel (Int, Int) (Future Int)
sumacostosas6 c1 c2 = proc (i1, i2) -> do
    f1 <- SkStrict c1 -< i1
    f2 <- SkStrict c2 -< i2
    returnA -< f1 + f2

