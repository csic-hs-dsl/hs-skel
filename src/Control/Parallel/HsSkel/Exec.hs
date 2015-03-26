{-# LANGUAGE GADTs #-}
{-# LANGUAGE Arrows #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}

module Control.Parallel.HsSkel.Exec (
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
import Control.Concurrent.STM (readTBQueue, tryReadTBQueue, atomically, writeTBQueue, newTBQueueIO, TBQueue)
import Control.DeepSeq (NFData, rnf)
import Control.Exception (evaluate)
import Control.Monad (liftM)
import Prelude hiding (id, mapM, mapM_, take, (.))


import Data.Time.Clock (getCurrentTime, diffUTCTime)

data IOFuture a = Later (MVar a)
readFuture :: IOFuture a -> IO a
readFuture (Later mvar) = readMVar mvar

instance Show a => Show (IOFuture a) where
    show (Later _) = "Later ?"

instance NFData (IOFuture a) where
    rnf _ = ()

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

execStream :: IOEC -> Stream dim IOFuture i -> IO (TBQueue (Maybe (S.Seq i)), TBQueue BackMsg)
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
                            atomically $ writeTBQueue qo (Just $ S.singleton v)
                            recc qo bqi i'
                        Nothing -> atomically $ writeTBQueue qo Nothing
                Just Stop -> return ()

--execStream (StMap (StMap stream' cons') cons) =  execStream (StMap stream' (cons . cons'))

execStream ec (StMap _ sk stream) = do
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
                            vo <- return =<< exec ec (skMap sk) vi
                            atomically $ writeTBQueue qo (Just vo)
                            recc qi qo bqi bqo
                        Nothing -> atomically $ writeTBQueue qo Nothing
            handleBackMsg continue bqi bqo

execStream ec (StParMap _ sk stream) = do
--    print "0"
    qo1 <- newTBQueueIO (queueLimit ec)
    bqi1 <- newTBQueueIO (queueLimit ec)
    qo2 <- newTBQueueIO (queueLimit ec)
    bqi2 <- newTBQueueIO (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    _ <- forkIO $ recc1 qi qo1 bqi1 bqo
    _ <- forkIO $ recc2 qo1 qo2 bqi2 bqi1
    return (qo2, bqi2)
    where 
        recc1 qi qo bqi bqo = do
--            print "1"
            let 
                continue = do
--                    print "2"
                    res <- atomically $ readTBQueue qi
                    case res of 
                        Just vi -> do
--                            print "3"
                            vo <- exec ec (SkPar (SkMap sk)) vi
                            atomically $ writeTBQueue qo (Just vo)
                            recc1 qi qo bqi bqo
                        Nothing -> atomically $ writeTBQueue qo Nothing
            handleBackMsg continue bqi bqo
        recc2 qi qo bqi bqo = do
            let 
                continue = do
                    res <- atomically $ readTBQueue qi
                    case res of 
                        Just vi -> do
                            vo <- exec ec SkSync vi
                            atomically $ writeTBQueue qo (Just vo)
                            recc2 qi qo bqi bqo
                        Nothing -> atomically $ writeTBQueue qo Nothing
            handleBackMsg continue bqi bqo


execStream ec (StChunk dim stream) = do
    t1 <- getCurrentTime
    qo <- newTBQueueIO (queueLimit ec)
    bqi <- newTBQueueIO (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    let chunkSize = dimLinearSize dim
    _ <- forkIO $ recc qi qo bqi bqo S.empty chunkSize t1
    return (qo, bqi)
    where 
        recc qi qo bqi bqo storage chunkSize t1 = do
            let 
                continue = do
                    i <- atomically $ readTBQueue qi
                    case i of
                        Just vi -> do
                            let storage' = storage S.>< vi
                            if (S.length storage' == chunkSize) 
                                then do
                                    atomically $ writeTBQueue qo (Just $ storage')
                                    t2 <- getCurrentTime
                                    --print $ diffUTCTime t2 t1
                                    recc qi qo bqi bqo S.empty chunkSize t2
                                else do
                                    recc qi qo bqi bqo storage' chunkSize t1
                        Nothing -> do
                            if (S.length storage > 0)
                                then atomically $ writeTBQueue qo (Just $ storage)
                                else return ()
                            atomically $ writeTBQueue qo Nothing
            handleBackMsg continue bqi bqo

execStream ec (StUnChunk _ stream) = do
    let chunk = dimHead . stDim $ stream
    qo <- newTBQueueIO (queueLimit ec)
    bqi <- newTBQueueIO (queueLimit ec)
    (qi, bqo) <- execStream ec stream
    _ <- forkIO $ recc qi qo bqi bqo chunk
    return (qo, bqi)
    where 
        recc qi qo bqi bqo chunk = do
            let 
                continue = do
                    i <- atomically $ readTBQueue qi
                    case i of
                        Just vsi -> do
                            let maxChunkIdx = div (S.length vsi) chunk
                            mapM_ (\c -> atomically . writeTBQueue qo . Just . S.take chunk . S.drop (c * chunk) $ vsi) [0 .. maxChunkIdx]
                            recc qi qo bqi bqo chunk
                        Nothing -> do
                            atomically $ writeTBQueue qo Nothing
            handleBackMsg continue bqi bqo

execStream ec (StStop _ skF z skCond stream) = do
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
                                        then atomically $ writeTBQueue qo (Just $ S.take (pos - 1) vi)
                                        else return ()
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
--    print "inicioPar"
    mVar <- newEmptyMVar
    _ <- forkIO (stuff i mVar)
    return $ Later mVar)
    where
        stuff i mVar = do
            r <- exec ec sk i
--            print "finPar"
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
            res <- atomically $ readTBQueue qi
            case res of
                Just vi -> do
                    z' <- foldlM (\r d -> exec ec red (r, d)) z vi
                    reducer qi z'
                Nothing -> do 
                    return z
{-
execSkParMap :: IOEC -> Skel IOFuture i o -> S.Seq i -> IO (IOFuture (S.Seq o))
execSkParMap ec sk = \s -> (do
    mVar <- newEmptyMVar
    _ <- forkIO (stuff s mVar)
    return $ Later mVar)
    where
        stuff s mVar = do
            let r = mapM (exec ec sk) s
            putMVar mVar r
            -}