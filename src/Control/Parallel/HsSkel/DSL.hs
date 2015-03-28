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
    DIM(..),
    Z(),
    (:.)(),
    dimHead,
    dimTail,
    Future(),
    Skel(..),
    Stream(..),
    stDim,
    -- * Execution
    Exec(Context, FutureImpl, exec),
    -- * Skeleton Smart Constructors
    skSeq,
    SkForkSupport(skFork),
    skSync,
    skMap,
    skRed,
    -- * Stream Smart Constructors
    StGenSupport(stGen),
    stMap,
    stParMap,
    stChunk,
    stUnChunk,
    stStop,
    -- * Miscellaneous
    stFromList,
    skConst,
    skMapF,
    skPairF,
    skTraverseF,
    skDaC
) where

import Data.Traversable (Traversable)
import Control.Arrow (Arrow(arr, first, second, (***)), ArrowChoice(left, right, (+++)), ArrowApply(app), returnA)
import Control.Category (Category, id, (.))
import Control.DeepSeq (NFData)
import Prelude (Bool, Either, Int, Maybe(Just, Nothing), ($), Show, Read, Eq, Ord, (*))


{- ================================================================== -}
{- ============================= Types ============================== -}
{- ================================================================== -}


-- | This Class is used to define which types are valid dimensions. Dimensions are used to keep track of the chunk size of a 'Stream'.
class DIM dim where
    dimLinearSize :: dim -> Int

-- | This type represents a 0-dimensional chunk (an element)
data Z = Z

-- | This type represents an N-dimensional chunk (where N > 0)
data tail :. head
    = !tail :. !head
    deriving (Show, Read, Eq, Ord)
infixl 3 :.

instance DIM Z where
    dimLinearSize _ = 1

instance DIM dim => DIM (dim :. Int) where
    dimLinearSize (tail :. head) = head * (dimLinearSize tail)

-- | An accessor to the head of the ':.' type
dimHead :: (DIM dim) => (dim :. Int) -> Int
dimHead (_ :. head) = head

-- | An accessor to the tail of the ':.' type
dimTail :: (DIM dim) => (dim :. Int) -> dim
dimTail (tail :. _) = tail

-- | This class is used to define which types represent a computation that is running in background.
-- Is not possible to handle Futures directly and you need to use them inside the Skeletons DSL.
class Future (f :: * -> *)

-- | This data type represents the core of the Skeleton DSL.
-- 
-- A Skeleton is an abstraction over functions that allows to create common solutions to common problems. A type of @Skel i o@ should be read as "a Skeleton that takes a value of type @i@ and generate an output of type @o@".
-- 
-- For every data type constructor that does not have an arrow notation equivalent, there is a function with the same signature and name, but with the first letter in lowercase.
--
-- The data type constructors are exposed to other modules only to allow the definition of multiple execution schedulers, but the design of the DSL is focused to take advantage of the expression power of the arrow extension.
-- 
-- If you are not developing a new execution scheduler, then you should use the smart constructors and the arrow notation instead of the data type constructors. To ensure that, add an import to 'Control.Parallel.HsSkel' instead of 'Control.Parallel.HsSkel.DSL'.
-- 
-- The constructors used in the arrow notation are:
-- 
-- * 'SkSeq_' is equivalent to 'arr' of 'Arrow'.
-- * 'SkComp' is equivalent to '.' of 'Category'
-- * 'SkPair' is equivalent to '***' of 'Arrow'
-- * 'SkChoice' is equivalent to '+++' of 'ArrowChoice' and is used in the if statement on the arrow notation.
-- * 'SkApply' is equivalent to 'app' of 'ArrowApply' and is used with the -<< operator in the arrow notation.
data Skel f i o where

    SkSeq    :: (NFData o, Future f) => (i -> o) -> Skel f i o
    SkSeq_   :: (Future f) => (i -> o) -> Skel f i o
    
    SkFork   :: (Future f) => Skel f i o -> Skel f i (f o)
    SkSync   :: (Future f) => Skel f (f i) i
    SkComp   :: (Future f) => Skel f x o -> Skel f i x -> Skel f i o
    SkPair   :: (Future f) => Skel f i1 o1 -> Skel f i2 o2 -> Skel f (i1, i2) (o1, o2)

    SkMap    :: (Traversable t, Future f) => Skel f i o -> Skel f (t i) (t o)
    SkChoice :: (Future f) => Skel f i o -> Skel f i' o' -> Skel f (Either i i') (Either o o')
    SkApply  :: (Future f) => Skel f (Skel f i o, i) o

    SkRed    :: (DIM dim, Future f) => Skel f (o, i) o -> Stream dim f i -> Skel f o o
    

-- | A Stream is like a pipeline that receives a flow of data and each single data passes across multiple parallel stages.
-- 
-- Every stage of the pipeline will be executed in parallel and it is responsibility of the execution scheduler to implement some communication between the stages in order to avoid one stage to produce data faster than the next stage can process. 
--
-- The first stage of a Streams will be generated by the 'StGen' constructor. This constructor is the link between the user code and the Stream world.
--
-- A Stream should not be infinite and it is the programmer who must ensure the end of the Stream. a Stream can be finished by returning Nothing in the function of 'StGen' or by adding a 'StStop' stage that returns true when some condition is met. See the doc of 'stGen' and 'stStop' for more details.
--
-- The Stream and Skel worlds are connected by the 'SkRed' constructor, which is similar to a fold over the stream applying an accumulator function and returning the result inside a Skeleton.
data Stream dim f d where
    StGen     :: (NFData o, Future f) => (i -> (Maybe (o, i))) -> i -> Stream Z f o
    StMap     :: (DIM dim, Future f) => dim -> Skel f i o -> Stream dim f i -> Stream dim f o
    StChunk   :: (DIM dim, Future f) => (dim :. Int) -> Stream dim f i -> Stream (dim:.Int) f i
    StUnChunk :: (DIM dim, Future f) => dim -> Stream (dim:.Int) f i -> Stream dim f i
    StParMap  :: (DIM dim, Future f) => dim -> Skel f i o -> Stream dim f i -> Stream dim f o
    StStop    :: (DIM dim, Future f) => dim -> Skel f (c, i) c -> c -> Skel f c Bool -> Stream dim f i -> Stream dim f i

-- | An accessor to the dimension of a 'Stream'
stDim :: Stream dim f d -> dim
stDim (StGen _ _) = Z
stDim (StMap dim _ _) = dim
stDim (StParMap dim _ _) = dim
stDim (StChunk dim _) = dim
stDim (StUnChunk dim _) = dim
stDim (StStop dim _ _ _ _) = dim


{- ================================================================== -}
{- ======================= Category and Arrow ======================= -}
{- ================================================================== -}

instance (Future f) => Category (Skel f) where
    id  = SkSeq_ id
    (.) = SkComp

instance (Future f) => Arrow (Skel f) where
    arr = SkSeq_
    first sk = sk `SkPair` id
    second sk = id `SkPair` sk
    (***) = SkPair
    --(&&&) sk1 sk2 = idem to default implementation.

instance (Future f) => ArrowChoice (Skel f) where
    left sk = SkChoice sk id
    right sk = SkChoice id sk
    (+++) = SkChoice
    --(|||) sk1 sk2 = idem to default implementation.

instance (Future f) => ArrowApply (Skel f) where
    app = SkApply


{- ================================================================== -}
{- ======================= Smart Constructors ======================= -}
{- ================================================================== -}

-- | Smart constructor for 'SkSeq'. This and 'arr' functions are the link between the user code and the Skeleton DSL.
-- 
-- This constructor creates a Skeleton that fully evaluates its output.
-- 
-- The main use of this constructor is to pass it to the 'skFork' to ensure when the 'Future' is ready to be read then its value is also fully evaluated.
skSeq :: (NFData o, Future f) => (i -> o) -- ^ A user defined function. 'NFData' is required for fully evaluates the output.
    -> Skel f i o
skSeq = SkSeq

-- | Smart constructor for 'SkSync'.
--
-- This constructor blocks until the input 'Future' is completed. It is also the only way to get the value from a 'Future'.
skSync :: (Future f) => Skel f (f i) i
skSync = SkSync

-- | Smart constructor for 'SkMap'.
-- 
-- This constructor is like 'Functor.fmap' but inside the Skeleton DSL. It takes a 'Traversable' structure and applies the Skeleton parameter to each element in the structure.
skMap :: (Traversable t, Future f) => Skel f i o -> Skel f (t i) (t o)
skMap = SkMap

-- | Smart constructor for 'SkRed'.
--
-- This constructor takes an accumulative Skeleton and a Stream and creates a Skeleton that takes an initial value and applies the accumulative Skeleton to each value of the Stream, returning the accumulated value.
--
-- Is like a fold for Stream but inside of the Skeleton DSL.
skRed :: (DIM dim, Future f) => Skel f (o, i) o -> Stream dim f i -> Skel f o o
skRed = SkRed

-- | Smart constructor for 'StMap'.
--
-- Takes a Skeleton and a Stream and returns a Stream with the same stages plus a new end stage that applies the Skeleton parameter to each value.
stMap :: (DIM dim, Future f) => Skel f i o -> Stream dim f i -> Stream dim f o
stMap sk st = StMap (stDim st) sk st

-- | Smart constructor for 'StChunk'.
--
-- Takes a size and a Stream a returns a Stream with the same stages plus a new end stage that collect @size@ values and put them in a vector.
-- Is useful if the work for processing each single value for the next stages is lower than the framework overhead.
stChunk :: (DIM dim, Future f) => Int -> Stream dim f i -> Stream (dim :. Int) f i
stChunk size st = StChunk ((stDim st) :. size) st

-- | Smart constructor for 'StUnChunk'.
--
-- Reverts a previous 'stChuck' application by adding a new stage that takes a 'Vector' of values and put each single value in the stream.
stUnChunk :: (DIM dim, Future f) => Stream (dim :. Int) f i -> Stream dim f i
stUnChunk st = StUnChunk (dimTail . stDim $ st) st

-- | Smart constructor for 'StParMap'.
--
-- Takes a Skeleton and a Stream and returns a Stream with the same stages plus a new end stage that applies the Skeleton parameter to each value. Each Chunk is evaluated in parallel.
stParMap :: (DIM dim, Future f) => Skel f i o -> Stream dim f i -> Stream dim f o
stParMap sk st = StParMap (stDim st) sk st

-- | Smart constructor for 'StStop'.
--
-- This constructor allow to stop the Stream outside the generator function used in the 'stGen'.
-- 
-- Is useful for problems in which put the logic of stop the stream in the generator should be too complex. For example, generate a Stream with the first 50 Fibonacci prime numbers. Using this constructor is possible to create a Stream which its generator gives Fibonacci numbers, the next stage verifies if the number is prime, and the next stop stage verifies if we have already 50 primes numbers.
stStop :: (DIM dim, Future f) => Skel f (c, i) c -> c -> Skel f c Bool -> Stream dim f i -> Stream dim f i
stStop acc z cond st = StStop (stDim st) acc z cond st

-- | This class allows to use the same function name for create a Skeleton from a function or another Skeleton.
class (Future f) => SkForkSupport f a i o where
    -- | Smart constructor for 'SkFork'.
    -- 
    -- This constructor runs the computation parameter in background. This allows to get parallelism by running multiple computations in background.
    skFork :: a i o -- ^ A function (@i -> o@) or Skeleton (@Skel i o@)
	    -> Skel f i (f o)
    
instance (Future f) => SkForkSupport f (Skel f) i o where
    skFork = SkFork

instance (NFData o, Future f) => SkForkSupport f (->) i o where
    skFork = SkFork . SkSeq

-- | This class allows to use the same function name to create a Stream from a function that returns a Maybe directly or not.
class (Future f) => StGenSupport f fun i o where
    -- | Smart constructor for 'StGen'. This is the first stage for all Streams.
    --
    -- It takes an initial seed and a generator function that receives a seed and returns a pair with a value and another seed (see the instances). The stream is constructed by sequentially applying the generator function for the initial seed and the next generated seeds until the function returns 'Nothing'.
    stGen :: fun -> i -> Stream Z f o

instance (NFData o, Future f) => StGenSupport f (i -> (Maybe (o, i))) i o where
    stGen = StGen

instance (NFData o, Future f) => StGenSupport f (i -> (o, i)) i o where
    stGen f = StGen (Just . f)


{- ================================================================== -}
{- ======================= Execution Context ======================== -}
{- ================================================================== -}

class Exec m where
    type Context m :: *
    type FutureImpl m :: * -> *
    exec :: (Future (FutureImpl m)) => Context m -> Skel (FutureImpl m) i o -> i -> m o


{- ================================================================== -}
{- ========================= Util Functions ========================= -}
{- ================================================================== -}

-- | Creates a Stream from a List. Requires 'NFData' of its elements in order to fully evaluate them.
stFromList :: (NFData a, Future f) => [a] -> Stream Z f a
stFromList l = StGen go l
    where
        go [] = Nothing
        go (x:xs) = Just (x, xs)


-- | Analogue to the 'Prelude.const' function but inside of Skeleton DSL. Returns a constant Skeleton that ignores the input and returns the parameter for any input.
skConst :: (Future f) => o -- ^ The constant value to be returned by the Skeleton.
    -> Skel f i o
skConst o = SkSeq_ (\_ -> o)

-- | This function allows to await for a future value and then execute another Skeleton, but without blocking the caller.
-- 
-- The blocking of the result and the application of the Skeleton will be done in the background.
skMapF :: (NFData o, Future f) => Skel f i o -> Skel f (f i) (f o)
skMapF sk = skFork $ sk . skSync

-- | This functions transform a pair of Futures into a Future of pairs.
skPairF :: (Future f) => Skel f (f o1, f o2) (f (o1, o2))
skPairF = skFork $ (***) skSync skSync

-- | This function transforms a 'Traversable' structure of Futures into a Future of the structure.
-- 
-- For example, using lists it would transform a @[Future o]@ in a @Future [o]@.
skTraverseF :: (Traversable t, Future f) => Skel f (t (f o)) (f (t o))
skTraverseF = skFork $ skMap skSync

-- | A complex Skeleton composition example.
-- 
-- This function returns a Skeleton implements the Divide & Conquer pattern. It takes a Skeleton that solves the base problem, a function to identify if the input is the base case, and a split and merge function.
skDaC :: (Traversable t, Future f) => 
    Skel f i o -- ^ Skeleton that resolves the base case.
    -> (i -> Bool) -- ^ A function that say if the input is the base case.
    -> (i -> t i) -- ^ A divide function strategy.
    -> (i -> t o -> o) -- ^ A merge function strategy.
    -> Skel f i o -- ^ A Skeleton that applies the D&C pattern.
skDaC skel isTrivial split combine = proc i -> do
    if (isTrivial i) 
        then
            skel -< i
        else do
            oSplit <- skMap skSync . skMap (skFork (skDaC skel isTrivial split combine)) -< split i
            returnA -< combine i oSplit
