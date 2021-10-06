{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
module DB where

import           ProjectM36.Client
import           ProjectM36.Tupleable
import           Data.Map.Strict as Map
import           ProjectM36.DataFrame
import           Data.UUID (UUID, toText, fromText)
import           Data.Either (isRight, isLeft)
import           Control.Monad.Except
import           Control.Monad.Reader
import qualified Codec.Winery as W
import           GHC.Generics
import           Control.DeepSeq
import           Data.Text hiding (length)
import           Data.Proxy

instance Atomable UUID where
  toAtom = TextAtom . toText

  fromAtom (TextAtom b) = case fromText b of
    Just x  -> x
    Nothing -> error "Invalid UUID Text"
  fromAtom _ = error "improper fromAtom"

  toAtomType _ = TextAtomType

  toAddTypeExpr _ = NoOperation

data DBEnv = DBEnv { getHead :: Text
                   , getConnection :: Connection
                   , getSessionId :: SessionId
                   }

type Action a = ReaderT DBEnv (ExceptT RelationalError IO) a

connInfo :: ConnectionInfo
connInfo = RemoteConnectionInfo
  "test-db"
  "127.0.0.1"
  "6543"
  emptyNotificationCallback

conn :: IO Connection
conn = handleIOError $ connectProjectM36 connInfo

handleIOError :: Show e => IO (Either e a) -> IO a
handleIOError m = do
  v <- m
  handleError v

handleError :: Show e => Either e a -> IO a
handleError eErr = case eErr of
  Left err -> print err >> error "Died due to errors."
  Right v  -> pure v

data DBAttrib = DBAttrib { attribIndex :: Int
                         , attribKey :: AttributeName
                         , attribIsUnique :: Bool
                         }
  deriving (Generic, Show, Eq, NFData, Atomable, Tupleable)
  deriving W.Serialise via W.WineryVariant DBAttrib

data TableSchema = TableSchema { _id :: UUID
                               , _doc :: Maybe Text
                               , tsIdent :: Text
                               , tsOwnerLogin :: Text
                               , tsRelVarName :: Maybe RelVarName
                               , tsAttribs :: [DBAttrib]
                               }
  deriving (Generic, Show, Eq, NFData, Atomable, Tupleable)
  deriving W.Serialise via W.WineryVariant TableSchema

runDB :: Action a -> IO (Either RelationalError a)
runDB a = do
  c <- liftIO conn
  sessionId <- liftIO $ createSessionAtHead c "master"
  case sessionId of
    Right sid -> withTransaction
      sid
      c
      (runExceptT
         (runReaderT
            a
            DBEnv { getHead = "master", getConnection = c, getSessionId = sid }))
      (autoMergeToHead sid c UnionMergeStrategy "master")
    Left e    -> return $ Left e

commitDB :: Action ()
commitDB = execGraphExp Commit

execDataFrameExp :: DataFrameExpr -> Action DataFrame
execDataFrameExp frameExp = do
  env <- ask
  lift
    $ ExceptT
    $ executeDataFrameExpr (getSessionId env) (getConnection env) frameExp

execGraphExp :: TransactionGraphOperator -> Action ()
execGraphExp graphOp = do
  env <- ask
  lift
    $ ExceptT
    $ executeGraphExpr (getSessionId env) (getConnection env) graphOp

execDBContextExp :: DatabaseContextExpr -> Action ()
execDBContextExp dbExp = do
  env <- ask
  lift
    $ ExceptT
    $ executeDatabaseContextExpr (getSessionId env) (getConnection env) dbExp

execDBContextIOExp :: DatabaseContextIOExpr -> Action ()
execDBContextIOExp dbIOExp = do
  env <- ask
  lift
    $ ExceptT
    $ executeDatabaseContextIOExpr
      (getSessionId env)
      (getConnection env)
      dbIOExp

genTestData :: IO (Either RelationalError ())
genTestData = runDB
  $ do
    execDBContextExp $ toAddTypeExpr (Proxy :: Proxy DBAttrib)
    execDBContextIOExp
      (CreateArbitraryRelation
         "test"
         [ NakedAttributeExpr (Attribute "_id" TextAtomType)
         , NakedAttributeExpr
             (Attribute
                "_doc"
                (ConstructedAtomType
                   "Maybe"
                   (Map.fromList [("a", TextAtomType)])))
         , NakedAttributeExpr (Attribute "tsIdent" TextAtomType)
         , NakedAttributeExpr (Attribute "tsOwnerLogin" TextAtomType)
         , NakedAttributeExpr
             (Attribute
                "tsRelVarName"
                (ConstructedAtomType
                   "Maybe"
                   (Map.fromList [("a", TextAtomType)])))
         , NakedAttributeExpr
             (Attribute
                "tsAttribs"
                (ConstructedAtomType
                   "List"
                   (Map.fromList
                      [("a", ConstructedAtomType "DBAttrib" Map.empty)])))]
         (200000, 200000))
    commitDB

getTestData :: IO (Either RelationalError Int)
getTestData = runDB
  $ do
    df <- execDataFrameExp
      DataFrameExpr { convertExpr = RelationVariable "test" ()
                    , orderExprs = [AttributeOrderExpr "_id" AscendingOrder]
                    , offset = Just 100000
                    , limit = Just 100
                    }
    return $ length $ tuples df
