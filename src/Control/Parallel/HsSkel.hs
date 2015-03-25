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
    ExecutionContext(exec),
    -- Constructors:
    skSeq,
    skPar,
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
