module Control.Parallel.HsSkel (
    -- Types:
    Future(),
    Skel(),
    Stream(),
    -- Constructors:
    skSeq,
    skPar,
    skSync,
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

import Control.Parallel.HsSkel.DSL
