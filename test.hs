{-# LANGUAGE OverloadedStrings #-}

import           Data.ByteString       (ByteString)
import qualified Data.ByteString       as B
import qualified Data.ByteString.Char8 as BC
import           System.Directory      (createDirectory, removeDirectoryRecursive,
                                        withCurrentDirectory)
import           System.IO             (writeFile)
import           Test.Tasty            (TestTree, defaultMain, testGroup, withResource)
import           Test.Tasty.HUnit      (testCase, testCaseSteps, (@?), (@?=))

import           Fh                    (fh', _path)
import           Hashing


main :: IO ()
main = defaultMain $
  testGroup "Tests"
    [ testHashing
    , withResource mkFileTree rmFileTree testFh
    ]


-- * Hashing

emptySHA1Key, emptySHA1EKey, emptyHexHash, emptyBinHash :: ByteString

emptySHA1Key  = ".git/annex/objects/Jx/11/SHA1-s0--"  <> emptyHexHash <> "/SHA1-s0--"  <> emptyHexHash
emptySHA1EKey = ".git/annex/objects/G9/X9/SHA1E-s0--" <> emptyHexHash <> "/SHA1E-s0--" <> emptyHexHash <> ".txt"

emptyHexHash = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
emptyBinHash = "\218\&9\163\238^kK\r2U\191\239\149`\CAN\144\175\216\a\t"

testHashing :: TestTree
testHashing = testGroup "Hashing tests"
    [ testCase "getAnnexSizeAndHash" $ do
        let getSzHashLink = getAnnexSizeAndHash True
        getSzHashLink emptySHA1Key  @?= Just (0, emptyBinHash)
        getSzHashLink emptySHA1EKey @?= Just (0, emptyBinHash)
        getSzHashLink (emptySHA1Key <> ".txt") @?= Nothing
        let Just k = B.stripSuffix "txt" emptySHA1EKey
        getSzHashLink k @?= Nothing
        let Just k' = B.stripSuffix ".txt" emptySHA1EKey
        getSzHashLink k' @?= Just (0, emptyBinHash)
        -- test endOfInput
        getSzHashLink (emptySHA1Key  <> "/more") @?= Nothing
        getSzHashLink (emptySHA1EKey <> "/more") @?= Nothing

    , testCase "hexlify & unhexlify'" $ do
        hexlify emptyBinHash @?= BC.unpack emptyHexHash
        unhexlify' emptyHexHash @?= emptyBinHash
    ]


-- * file tree

testDir :: FilePath
testDir = "test-dir-t9pBD7EPYM"

mkFileTree :: IO FilePath
mkFileTree = do
  createDirectory testDir
  withCurrentDirectory testDir $ do
    createDirectory "a"
    writeFile "a/x" "x"
  return testDir

rmFileTree :: FilePath -> IO ()
rmFileTree = removeDirectoryRecursive


-- * fh

testFh :: IO FilePath -> TestTree
testFh dir = testGroup "fh tests"
    [ testCaseSteps "paths" $ \step -> do
        root <- dir
        withCurrentDirectory root $ do
          step "avoid double slash"
          do list <- fh' [".", "-R1"]
             not (any ("//" `B.isInfixOf`) $ map _path list) @? "has double slash"
    ]
