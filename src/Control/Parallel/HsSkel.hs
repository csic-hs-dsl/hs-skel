module Control.Parallel.HsSkel (
    -- Types:
    DIM(..),
    Z,
    (:.),
    dimHead,
    dimTail,
    Skel(..),
    Stream(..),
    stDim,
    Exec(Context, Future, exec),
    -- Constructors:
    skStrict,
    skFork,
    skSync,
    skMap,
    stParMap,
    skRed,
    stUnfoldr,
    stMap,
    stChunk,
    stUnChunk,
    stUntil,
    stFromList,
    -- Utils:
    skConst,
    skMapF,
    skPairF,
    skTraverseF,
    skDaC
) where

import Control.Parallel.HsSkel.DSL
