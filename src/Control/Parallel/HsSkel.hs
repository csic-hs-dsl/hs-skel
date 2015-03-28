module Control.Parallel.HsSkel (
    -- Types:
    DIM(..),
    Z,
    (:.),
    dimHead,
    dimTail,
    Future(),
    Skel(..),
    Stream(..),
    stDim,
    Exec(Context, FutureImpl, exec),
    -- Constructors:
    skSeq,
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
