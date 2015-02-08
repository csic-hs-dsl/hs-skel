module Control.Parallel.HsSkel (
    -- Types:
    Future(),
    Skel(),
    Stream(),
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
    stFromList,
    -- Utils:
    skConst,
    skMapF,
    skPairF,
    skTraverseF,
    skDaC
) where

import Control.Parallel.HsSkel.DSL
