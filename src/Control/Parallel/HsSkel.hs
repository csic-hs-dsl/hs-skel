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
    stFromList,
    -- Utils:
    skConst,
    skMapF,
    skPairF,
    skTraverseF,
    skDaC,
    skParFromFunc
) where

import Control.Parallel.HsSkel.DSL
