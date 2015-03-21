{-# LANGUAGE GADTs #-}
{-# LANGUAGE Arrows #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}

module Control.Parallel.HsSkel.Exec (
    IOEC(..),
    IOFuture()
) where

import Control.Parallel.HsSkel.DSL

import Data.Foldable (mapM_)
import Data.Traversable (mapM)
import Data.Vector (freeze)
import Data.Vector.Mutable (new, write, take)
import Control.Category ((.))
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar, readMVar)
import Control.Concurrent.STM (readTBQueue, tryReadTBQueue, atomically, writeTBQueue, newTBQueueIO, TBQueue)
import Control.DeepSeq (NFData, rnf)
import Control.Exception (evaluate)
import Control.Monad (liftM)
import Prelude hiding (id, mapM, mapM_, take, (.))


--import Data.Time.Clock (getCurrentTime, diffUTCTime)

newtype IOFuture a = IOFuture { mvar :: MVar a }

instance Future IOFuture

data IOEC = IOEC { queueLimit :: Int }

instance ExecutionContext IOEC IO IOFuture where
    exec = execIO

{- ================================================================== -}
{- =========================== Auxiliary ============================ -}
{- ================================================================== -}

eval :: (NFData a) => a -> IO a
eval a = do
    evaluate $ rnf a
    return a

data BackMsg = Stop

handleBackMsg :: IO () -> TBQueue BackMsg -> TBQueue BackMsg -> IO ()
handleBackMsg continue bqi bqo = do
    backMsg <- atomically $ tryReadTBQueue bqi
    case backMsg of
        Nothing -> continue
        Just Stop -> do
            atomically $ writeTBQueue bqo Stop

{- ================================================================== -}
{- ======================= Stream Execution ========================= -}
{- ================================================================== -}

execStream :: IOEC -> Stream IOFuture i -> IO (TBQueue (Maybe i), TBQueue BackMsg)
execStream ec (StGen gen i) = do
    qo <- newTBQueueIO (queueLimit ec)
    bqi <- newTBQueueIO (queueLimit ec)
    _ <- forkIO $ recc qo bqi i
    return (qo, bqi)
    where 
        recc qo bqi i = do
            backMsg <- atomically $ tryReadTBQueue bqi
            case backMsg of
                Nothing -> do
                    let res = gen i
                    case res of 
                        Just (v, i') -> do 
                            _ <- eval v
                            atomically $ writeTBQueue qo (Just v)
                            recc qo bqi i'
                        Nothing -> atomically $ writeTBQueue qo Nothing
                Just Stop -> return ()

--execStream (StMap (StMap stream' cons') cons) =  execStream (StMap stream' (cons . cons'))

execStream ec (StMap cons stream) = do
    qo <- newTBQueueIO (queueLimit ec)
    bqi <- newTBQueueIO (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    _ <- forkIO $ recc qi qo bqi bqo
    return (qo, bqi)
    where 
        recc qi qo bqi bqo = do
            let 
                continue = do
                    res <- atomically $ readTBQueue qi
                    case res of 
                        Just vi -> do 
                            vo <- exec ec cons vi
                            atomically $ writeTBQueue qo (Just vo)
                            recc qi qo bqi bqo
                        Nothing -> atomically $ writeTBQueue qo Nothing
            handleBackMsg continue bqi bqo

execStream ec (StChunk chunkSize stream) = do
    qo <- newTBQueueIO (queueLimit ec)
    bqi <- newTBQueueIO (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    storage <- new chunkSize
    _ <- forkIO $ recc qi qo bqi bqo storage 0
    return (qo, bqi)
    where 
        recc qi qo bqi bqo storage pos = do
            let 
                continue = do
                    i <- atomically $ readTBQueue qi
                    case i of
                        Just vi -> do
                            write storage pos vi
                            let newCant = pos + 1
                            if (newCant == chunkSize) 
                                then do
                                    vector <- freeze storage
                                    atomically $ writeTBQueue qo (Just vector)
                                    recc qi qo bqi bqo storage 0
                                else do
                                    recc qi qo bqi bqo storage newCant
                        Nothing -> do
                            if (pos > 0)
                                then do
                                    vector <- freeze $ take pos storage
                                    atomically $ writeTBQueue qo (Just vector)
                                else
                                    return ()
                            atomically $ writeTBQueue qo Nothing
            handleBackMsg continue bqi bqo

execStream ec (StUnChunk stream) = do
    qo <- newTBQueueIO (queueLimit ec)
    bqi <- newTBQueueIO (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    _ <- forkIO $ recc qi qo bqi bqo
    return (qo, bqi)
    where 
        recc qi qo bqi bqo = do
            let 
                continue = do
                    i <- atomically $ readTBQueue qi
                    case i of
                        Just vsi -> do
                            mapM_ (\v -> atomically $ writeTBQueue qo (Just v)) vsi
                            recc qi qo bqi bqo
                        Nothing -> do
                            atomically $ writeTBQueue qo Nothing
            handleBackMsg continue bqi bqo

execStream ec (StStop skF z skCond stream) = do
    qo <- newTBQueueIO (queueLimit ec)
    bqi <- newTBQueueIO (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    _ <- forkIO $ recc qi qo bqi bqo z
    return (qo, bqi)
    where 
        recc qi qo bqi bqo acc = do
            let 
                continue = do
                    i <- atomically $ readTBQueue qi
                    case i of
                        Just vi -> do
                            acc' <- exec ec skF (acc, vi)
                            cond <- exec ec skCond acc'
                            if cond
                                then do
                                    atomically $ writeTBQueue bqo Stop
                                    atomically $ writeTBQueue qo Nothing
                                else do
                                    atomically $ writeTBQueue qo (Just vi)
                                    recc qi qo bqi bqo acc'
                        Nothing -> do
                            atomically $ writeTBQueue qo Nothing
            handleBackMsg continue bqi bqo

{- ================================================================== -}
{- ======================== Skel Execution ========================== -}
{- ================================================================== -}

execIO :: IOEC -> Skel IOFuture i o -> i -> IO o
execIO _ (SkSeq f) = (eval =<<) . liftM f . return
execIO _ (SkSeq_ f) = liftM f . return
execIO ec (SkPar sk) = \i -> (do
    mVar <- newEmptyMVar
    _ <- forkIO (stuff i mVar)
    return $ IOFuture mVar)
    where
        stuff i mVar = do
            r <- exec ec sk i
            putMVar mVar r
execIO _ (SkSync) = takeMVar . mvar
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
            res <- atomically $ readTBQueue qi
            case res of
                Just v -> do
                    z' <- exec ec red (z, v)
                    reducer qi z'
                Nothing -> do 
                    return z

