{-# LANGUAGE GADTs #-}
{-# LANGUAGE Arrows #-}

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}

module Control.Parallel.HsSkel.DSL (
    -- Types:
    Future(..),
    Skel(..),
    Stream(..),
    -- Constructors:
    skSeq,
    skPar,
    skSync,
    skComp,
    skMap,
    skRed,
    stGen,
    stMap,
    stChunk,
    stUnChunk,
    stStop,
    stFromList,
    -- Utils:
    skConst,
    skMapF,
    skPairF,
    skTraverseF,
    skDaC
) where

import Data.Traversable (Traversable)
import Data.Vector (Vector)
import Control.Arrow (Arrow(arr, first, second, (***)), ArrowChoice(left, right, (+++)), ArrowApply(app), returnA)
import Control.Category (Category, id, (.))
import Control.Concurrent.MVar (MVar)
import Control.DeepSeq (NFData, rnf)
import Prelude (Bool, Either, Int, Maybe(Just, Nothing), Show(show), ($))

{- ================================================================== -}
{- ============================= Types ============================== -}
{- ================================================================== -}

newtype Future a = Future { mvar :: MVar a }

instance Show a => Show (Future a) where
    show _ = "Future ?"

instance NFData (Future a) where
    rnf _ = ()

infixr 9 `SkComp`
infixl 8 `SkPair`

data Skel i o where
    SkSeq   :: (NFData o) => (i -> o) -> Skel i o
    SkSeq_  :: (i -> o) -> Skel i o
    
    SkPar   :: Skel i o -> Skel i (Future o)
    SkSync  :: Skel (Future i) i
    SkComp  :: Skel x o -> Skel i x -> Skel i o
    SkPair  :: Skel i1 o1 -> Skel i2 o2 -> Skel (i1, i2) (o1, o2)

    SkMap   :: (Traversable t) => Skel i o -> Skel (t i) (t o)
    SkIf    :: Skel i o -> Skel i' o' -> Skel (Either i i') (Either o o')
    SkApply :: Skel (Skel i o, i) o

    SkRed   :: Skel (o, i) o -> Stream i -> Skel o o
    
    
data Stream d where
    StGen     :: NFData o => (i -> (Maybe (o, i))) -> i -> Stream o
    StMap     :: Skel i o -> Stream i -> Stream o
    StChunk   :: Int -> Stream i -> Stream (Vector i)
    StUnChunk :: Stream (Vector i) -> Stream i
    StStop    :: (c -> i -> c) -> c -> (c -> Bool) -> Stream i -> Stream i

{- ================================================================== -}
{- ======================= Category and Arrow ======================= -}
{- ================================================================== -}

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


{- ================================================================== -}
{- ======================= Smart Constructors ======================= -}
{- ================================================================== -}

skSeq :: (NFData o) => (i -> o) -> Skel i o
skSeq = SkSeq

skSync :: Skel (Future i) i
skSync = SkSync

skComp :: Skel x o -> Skel i x -> Skel i o
skComp = SkComp

skMap :: (Traversable t) => Skel i o -> Skel (t i) (t o)
skMap = SkMap
    
skRed :: Skel (o, i) o -> Stream i -> Skel o o
skRed = SkRed

stGen :: (NFData i, NFData o) => (i -> (Maybe (o, i))) -> i -> Stream o
stGen = StGen

stMap :: Skel i o -> Stream i -> Stream o
stMap = StMap

stChunk :: Int -> Stream i -> Stream (Vector i)
stChunk = StChunk

stUnChunk :: Stream (Vector i) -> Stream i
stUnChunk = StUnChunk

stStop :: (c -> i -> c) -> c -> (c -> Bool) -> Stream i -> Stream i
stStop = StStop

stFromList :: (NFData a) => [a] -> Stream a
stFromList l = StGen go l
    where
        go [] = Nothing
        go (x:xs) = Just (x, xs)

class SkParSupport a i o where
    skPar :: a i o -> Skel i (Future o)
    
instance SkParSupport Skel i o where
    skPar = SkPar

instance NFData o => SkParSupport (->) i o where
    skPar = skPar . skSeq


{- ================================================================== -}
{- ========================= Util Functions ========================= -}
{- ================================================================== -}

skConst :: o -> Skel i o
skConst o = SkSeq_ (\_ -> o)

skMapF :: (NFData o) => Skel i o -> Skel (Future i) (Future o)
skMapF sk = skPar $ sk . skSync

skPairF :: Skel (Future o1, Future o2) (Future (o1, o2))
skPairF = skPar $ (***) skSync skSync

skTraverseF :: (Traversable t) => Skel (t (Future o)) (Future (t o))
skTraverseF = skPar $ skMap skSync

skDaC :: (Traversable t) => Skel i o -> (i -> Bool) -> (i -> t i) -> (i -> t o -> o) -> Skel i o
skDaC skel isTrivial split combine = proc i -> do
    if (isTrivial i) 
        then
            skel -< i
        else do
            oSplit <- skMap skSync . skMap (skPar (skDaC skel isTrivial split combine)) -< split i
            returnA -< combine i oSplit

