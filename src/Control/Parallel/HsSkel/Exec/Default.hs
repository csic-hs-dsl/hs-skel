{-# LANGUAGE GADTs #-}
{-# LANGUAGE Arrows #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}

module Control.Parallel.HsSkel.Exec.Default (
    IOEC(..),
    IOFuture()
) where

import Control.Parallel.HsSkel.DSL

import Data.Foldable (mapM_, foldlM)
import Data.Traversable (mapM)
import qualified Data.Sequence as S
import Control.Category ((.))
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, readMVar)
import Control.DeepSeq (NFData, rnf)
import Control.Exception (evaluate)
import Control.Monad (liftM)
import Prelude hiding (id, mapM, mapM_, take, (.))

import Control.Concurrent.Chan.Unagi.Bounded (InChan, OutChan, newChan, readChan, writeChan, tryReadChan, tryRead)


--import Data.Time.Clock (getCurrentTime, diffUTCTime)

{- ================================================================== -}
{- ============================= Types ============================== -}
{- ================================================================== -}

data IOFuture a = Later (MVar a)
readFuture :: IOFuture a -> IO a
readFuture (Later mvar) = readMVar mvar

instance Future IOFuture

data IOEC = IOEC { queueLimit :: Int }

instance Exec IO where
    type Context IO = IOEC
    type FutureImpl IO = IOFuture
    exec = execIO


data Queue a = Queue { 
    inChan :: InChan a, 
    outChan :: OutChan a
}

newQueue :: Int -> IO (Queue a)
newQueue limit = do
    (inChan, outChan) <- newChan limit
    return $ Queue inChan outChan

readQueue :: Queue a -> IO a
readQueue = readChan . outChan

tryReadQueue :: Queue a -> IO (Maybe a)
tryReadQueue q = do
    elem <- tryReadChan $ outChan q
    tryRead elem

writeQueue :: Queue a -> a -> IO ()
writeQueue = writeChan . inChan


{- ================================================================== -}
{- =========================== Auxiliary ============================ -}
{- ================================================================== -}

eval :: (NFData a) => a -> IO a
eval a = do
    evaluate $ rnf a
    return a

data BackMsg = Stop

handleBackMsg :: IO () -> Queue BackMsg -> Queue BackMsg -> IO ()
handleBackMsg continue bqi bqo = do
    backMsg <- tryReadQueue bqi
    case backMsg of
        Nothing -> continue
        Just Stop -> writeQueue bqo Stop

{- ================================================================== -}
{- ======================= Stream Execution ========================= -}
{- ================================================================== -}

execStream :: IOEC -> Stream dim IOFuture i -> IO (Queue (Maybe (S.Seq i)), Queue BackMsg)
execStream ec (StGen gen i) = do
    qo <- newQueue (queueLimit ec)
    bqi <- newQueue (queueLimit ec)
    _ <- forkIO $ recc qo bqi i
    return (qo, bqi)
    where 
        recc qo bqi i = do
            backMsg <- tryReadQueue bqi
            case backMsg of
                Nothing -> do
                    let res = gen i
                    case res of 
                        Just (v, i') -> do 
                            _ <- eval v
                            writeQueue qo (Just $ S.singleton v)
                            recc qo bqi i'
                        Nothing -> writeQueue qo Nothing
                Just Stop -> return ()

execStream ec (StMap _ sk stream) = do
    qo <- newQueue (queueLimit ec)
    bqi <- newQueue (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    _ <- forkIO $ recc qi qo bqi bqo
    return (qo, bqi)
    where 
        recc qi qo bqi bqo = do
            let 
                continue = do
                    res <- readQueue qi
                    case res of 
                        Just vi -> do 
                            vo <- return =<< exec ec (skMap sk) vi
                            writeQueue qo (Just vo)
                            recc qi qo bqi bqo
                        Nothing -> writeQueue qo Nothing
            handleBackMsg continue bqi bqo

execStream ec (StChunk dim stream) = do
    qo <- newQueue (queueLimit ec)
    bqi <- newQueue (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    let chunkSize = dimLinearSize dim
    _ <- forkIO $ recc qi qo bqi bqo S.empty chunkSize
    return (qo, bqi)
    where 
        recc qi qo bqi bqo storage chunkSize = do
            let 
                continue = do
                    i <- readQueue qi
                    case i of
                        Just vi -> do
                            let storage' = storage S.>< vi
                            if (S.length storage' == chunkSize) 
                                then do
                                    writeQueue qo (Just $ storage')
                                    recc qi qo bqi bqo S.empty chunkSize
                                else do
                                    recc qi qo bqi bqo storage' chunkSize
                        Nothing -> do
                            if (S.length storage > 0)
                                then writeQueue qo (Just $ storage)
                                else return ()
                            writeQueue qo Nothing
            handleBackMsg continue bqi bqo

execStream ec (StUnChunk _ stream) = do
    let chunk = dimHead . stDim $ stream
    qo <- newQueue (queueLimit ec)
    bqi <- newQueue (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    _ <- forkIO $ recc qi qo bqi bqo chunk
    return (qo, bqi)
    where 
        recc qi qo bqi bqo chunk = do
            let 
                continue = do
                    i <- readQueue qi
                    case i of
                        Just vsi -> do
                            let maxChunkIdx = div (S.length vsi) chunk
                            mapM_ (\c -> writeQueue qo . Just . S.take chunk . S.drop (c * chunk) $ vsi) [0 .. maxChunkIdx]
                            recc qi qo bqi bqo chunk
                        Nothing -> do
                            writeQueue qo Nothing
            handleBackMsg continue bqi bqo

execStream ec (StParMap _ sk stream) = do
    qo1 <- newQueue (queueLimit ec)
    bqi1 <- newQueue (queueLimit ec)
    qo2 <- newQueue (queueLimit ec)
    bqi2 <- newQueue (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    _ <- forkIO $ recc1 qi qo1 bqi1 bqo
    _ <- forkIO $ recc2 qo1 qo2 bqi2 bqi1
    return (qo2, bqi2)
    where 
        recc1 qi qo bqi bqo = do
            let 
                continue = do
                    res <- readQueue qi
                    case res of 
                        Just vi -> do
                            vo <- exec ec (SkFork (SkMap sk)) vi
                            writeQueue qo (Just vo)
                            recc1 qi qo bqi bqo
                        Nothing -> writeQueue qo Nothing
            handleBackMsg continue bqi bqo
        recc2 qi qo bqi bqo = do
            let 
                continue = do
                    res <- readQueue qi
                    case res of 
                        Just vi -> do
                            vo <- exec ec SkSync vi
                            writeQueue qo (Just vo)
                            recc2 qi qo bqi bqo
                        Nothing -> writeQueue qo Nothing
            handleBackMsg continue bqi bqo

execStream ec (StStop _ skF z skCond stream) = do
    qo <- newQueue (queueLimit ec)
    bqi <- newQueue (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    _ <- forkIO $ recc qi qo bqi bqo z
    return (qo, bqi)
    where 
        recc qi qo bqi bqo acc = do
            let 
                continue = do
                    i <- readQueue qi
                    case i of
                        Just vi -> do
                            (acc', pos, stop) <- 
                                foldlM (\(a, p, cond) v -> 
                                    do -- esto no esta muy bien ya que recorre todo el arreglo
                                        if cond
                                            then do
                                                return (a, p, cond)
                                            else do
                                                a' <- exec ec skF (a, v)
                                                cond' <- exec ec skCond a'
                                                return (a', p + 1, cond')
                                    ) (acc, 0, False) vi
                            if stop
                                then do
                                    if pos > 0 
                                        then writeQueue qo (Just $ S.take (pos - 1) vi)
                                        else return ()
                                    writeQueue bqo Stop
                                    writeQueue qo Nothing
                                else do
                                    writeQueue qo (Just vi)
                                    recc qi qo bqi bqo acc'
                        Nothing -> do
                            writeQueue qo Nothing
            handleBackMsg continue bqi bqo

{- ================================================================== -}
{- ======================== Skel Execution ========================== -}
{- ================================================================== -}

execIO :: IOEC -> Skel IOFuture i o -> i -> IO o
execIO _ (SkSeq f) = (eval =<<) . liftM f . return
execIO _ (SkSeq_ f) = liftM f . return
execIO ec (SkFork sk) = \i -> (do
    mVar <- newEmptyMVar
    _ <- forkIO (stuff i mVar)
    return $ Later mVar)
    where
        stuff i mVar = do
            r <- exec ec sk i
            putMVar mVar r
execIO _ (SkSync) = readFuture
execIO ec (SkComp s2 s1) = (exec ec s2 =<<) . exec ec s1
execIO ec (SkPair sk1 sk2) = \(i1, i2) -> do
    o1 <- exec ec sk1 i1
    o2 <- exec ec sk2 i2
    return (o1, o2)
execIO ec (SkMap s) = mapM (exec ec s)
execIO ec (SkChoice sl sr) = \input ->
    case input of
        Left i -> exec ec sl i >>= return . Left
        Right i -> exec ec sr i >>= return . Right
execIO ec (SkApply) = \(sk, i) -> exec ec sk i
execIO ec (SkRed red stream) = \z -> do
    (qi, _) <- execStream ec stream   
    reducer qi z
    where 
        reducer qi z = do
            res <- readQueue qi
            case res of
                Just vi -> do
                    z' <- foldlM (\r d -> exec ec red (r, d)) z vi
                    reducer qi z'
                Nothing -> do 
                    return z
