{-# LANGUAGE GADTs #-}
{-# LANGUAGE Arrows #-}

module Control.Parallel.HsSkel.Exec (
    exec
    ) where

import Control.Parallel.HsSkel.DSL

import Data.Foldable (mapM_)
import Data.Traversable (mapM)
import Data.Vector (freeze)
import Data.Vector.Mutable (new, write, take)
import Control.Category ((.))
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.STM (readTBQueue, tryReadTBQueue, atomically, writeTBQueue, newTBQueueIO, TBQueue)
import Control.DeepSeq (NFData, rnf)
import Control.Exception (evaluate)
import Control.Monad (liftM)
import Prelude hiding (id, mapM, mapM_, take, (.))


--import Data.Time.Clock (getCurrentTime, diffUTCTime)

{- ================================================================== -}
{- =========================== Auxiliary ============================ -}
{- ================================================================== -}

queueLimit :: Int
queueLimit = 1000

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

execStream :: Stream i -> IO (TBQueue (Maybe i), TBQueue BackMsg)
execStream (StGen gen i) = do
    qo <- newTBQueueIO queueLimit
    bqi <- newTBQueueIO queueLimit
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

execStream (StMap cons stream) = do
    qo <- newTBQueueIO queueLimit
    bqi <- newTBQueueIO queueLimit
    (qi, bqo) <- execStream stream
    _ <- forkIO $ recc qi qo bqi bqo
    return (qo, bqi)
    where 
        recc qi qo bqi bqo = do
            let 
                continue = do
                    res <- atomically $ readTBQueue qi
                    case res of 
                        Just vi -> do 
                            vo <- exec cons vi
                            atomically $ writeTBQueue qo (Just vo)
                            recc qi qo bqi bqo
                        Nothing -> atomically $ writeTBQueue qo Nothing
            handleBackMsg continue bqi bqo

execStream (StChunk chunkSize stream) = do
    qo <- newTBQueueIO queueLimit
    bqi <- newTBQueueIO queueLimit
    (qi, bqo) <- execStream stream
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

execStream (StUnChunk stream) = do
    qo <- newTBQueueIO queueLimit
    bqi <- newTBQueueIO queueLimit
    (qi, bqo) <- execStream stream
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

execStream (StStop skF z skCond stream) = do
    qo <- newTBQueueIO queueLimit
    bqi <- newTBQueueIO queueLimit
    (qi, bqo) <- execStream stream
    _ <- forkIO $ recc qi qo bqi bqo z
    return (qo, bqi)
    where 
        recc qi qo bqi bqo acc = do
            let 
                continue = do
                    i <- atomically $ readTBQueue qi
                    case i of
                        Just vi -> do
                            acc' <- exec skF (acc, vi)
                            cond <- exec skCond acc'
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

exec :: Skel i o -> i -> IO o
exec (SkSeq f) = (eval =<<) . liftM f . return
exec (SkSeq_ f) = liftM f . return
exec (SkPar sk) = \i -> (do
    mVar <- newEmptyMVar
    _ <- forkIO (stuff i mVar)
    return $ Future mVar)
    where
        stuff i mVar = do
            --print ("SkPar inicio " ++ show i)
            r <- exec sk i
            --print ("SkPar fin " ++ show i)
            putMVar mVar r
exec (SkSync) = takeMVar . mvar
exec (SkComp s2 s1) = (exec s2 =<<) . exec s1
exec (SkPair sk1 sk2) = \(i1, i2) -> do
    o1 <- exec sk1 i1
    o2 <- exec sk2 i2
    return (o1, o2)
exec (SkMap s) = mapM (exec s)
exec (SkIf sl sr) = \input ->
    case input of
        Left i -> exec sl i >>= return . Left
        Right i -> exec sr i >>= return . Right
exec (SkApply) = \(sk, i) -> exec sk i
exec (SkRed red stream) = \z -> do
    (qi, _) <- execStream stream   
    reducer qi z
    where 
        reducer qi z = do
            res <- atomically $ readTBQueue qi
            case res of
                Just v -> do
                    z' <- exec red (z, v)
                    reducer qi z'
                Nothing -> do 
                    return z

