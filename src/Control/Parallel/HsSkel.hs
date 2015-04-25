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

import Control.Parallel.HsSkel.DSL
