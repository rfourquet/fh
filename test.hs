{-# LANGUAGE OverloadedStrings #-}

import           Control.Monad         (forM_)
import           Data.ByteString       (ByteString)
import qualified Data.ByteString       as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.UTF8  as B8
import           System.Directory      (createDirectory, createDirectoryIfMissing,
                                        removeDirectoryRecursive, withCurrentDirectory)
import           System.IO             (writeFile)
import           System.Process        (readProcessWithExitCode)
import           Test.Tasty            (TestTree, defaultMain, testGroup, withResource)
import           Test.Tasty.HUnit      (testCase, testCaseSteps, (@?), (@?=))

import           Fh                    (Entry, fh', longOptions, preParseArgs, shortOptions, _path,
                                        _size)
import           Hashing
import qualified RawPath               as R


main :: IO ()
main = defaultMain $
  testGroup "Tests"
    [ testHashing
    , testRawPath
    , withResource mkFileTree rmFileTree testFh
    , testOptParse
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


-- * RawPath

testRawPath :: TestTree
testRawPath = testCaseSteps "RawPath tests" $ \step -> do
                step "combine'"
                do R.combine' ""    "b"  @?= "b"
                   R.combine' ""    "/b" @?= "/b"
                   R.combine' "a/"  "b"  @?= "a/b"
                   R.combine' "a//" "b"  @?= "a//b"
                   R.combine' "a/"  "/b" @?= "a//b"
                   R.combine' "a"   "b"  @?= "a/b"
                   R.combine' "a"   "/b" @?= "a//b"
                step "combine"
                do R.combine  "a"   ""   @?= "a"
                   R.combine  "a"   "b"  @?= "a/b"
                   R.combine  "a/"  "b"  @?= "a/b"
                   R.combine  "a"   "/b" @?= "/b"
                   R.combine  "a"   ""   @?= "a"
                   R.combine  "a"   ""   @?= "a"


-- * file tree

testDir :: FilePath
testDir = "test-dir-t9pBD7EPYM"

mkFileTree :: IO FilePath
mkFileTree = do
  createDirectory testDir
  withCurrentDirectory testDir $ do
    createDirectory "a"
    writeFile "a/x" "x"
    writeFile "a/y" "yy"
    createDirectory "broken"
    writeFile "broken/Mod\232les" ""
    writeFile "broken/Mod\56552les" ""
  return testDir

rmFileTree :: FilePath -> IO ()
rmFileTree = removeDirectoryRecursive


-- * fh

-- UTF8 encoding is applied to the paths in args
fh'' :: [String] -> IO [Entry]
fh'' = flip fh' []

testFh :: IO FilePath -> TestTree
testFh dir = testGroup "fh tests"
    [ testCaseSteps "paths" $ \step -> do
        root <- dir
        withCurrentDirectory root $ do
          step "avoid double slash"
          forM_ [".", "./"] $ \d -> do
             list <- fh'' [d, "-R1"]
             not (any ("//" `B.isInfixOf`) $ map _path list) @? "has double slash"

          step "broken encoding"
          (_, _, err) <- readProcessWithExitCode "sh" ["-c", "fh broken/*"] []
          err @?= ""
    , testCaseSteps "fh'" $ \step -> do
        root <- dir
        withCurrentDirectory root $ do
          step "sorting"
          r1 <- fh'' ["-s", "a/y", "a/x"]
          map _size r1 @?= [2, 1]
          r2 <- fh'' ["-sS", "a/y", "a/x"]
          map _size r2 @?= [1, 2]
          r3 <- fh'' ["-sSc", "a/y", "a/x"]
          map _size r3 @?= [3, 1, 2]
          map _path r3 @?= ["*total*", "a/x", "a/y"]

          step "sort-first"
          createDirectoryIfMissing True "first/a/b"
          writeFile "first/a/b/x" "xxx"
          writeFile "first/a/y" "yy"
          f1 <- fh'' ["-sS", "first/a/", "first/a/b/"]
          map _size f1 @?= [3, 5]
          f2 <- fh'' ["-sS", "-u", "first/a/", "first/a/b/"]
          map _size f2 @?= [5]
          f3 <- fh'' ["-sS", "-uq", "first/a/", "first/a/b/"]
          map _size f3 @?= [3, 2] -- cf. README.md

          step "cache level"
          createDirectoryIfMissing True "cache/level"
          m1 <- fh'' ["-s", "cache"]
          map _size m1 @?= [0]
          writeFile "cache/level/x" "x"
          m2 <- fh'' ["-s", "-l2", "cache"]
          map _size m2 @?= [0]
          m3 <- fh'' ["-s", "cache"] -- default cache level: 2
          map _size m3 @?= [0]
          m4 <- fh'' ["-s", "-l1", "cache"]
          map _size m4 @?= [1]
          writeFile "cache/x" "x"
          m5 <- fh'' ["-s", "-l3", "cache"]
          map _size m5 @?= [1]
          m6 <- fh'' ["-s", "-l2", "cache"]
          map _size m6 @?= [2]

          step "no-update-db"
          createDirectoryIfMissing True "no-update/db"
          n1 <- fh'' ["--no-update-db", "-s", "no-update/"]
          map _size n1 @?= [0]
          writeFile "no-update/db/x" "x"
          n2 <- fh'' ["--no-update-db", "-s", "-l2", "no-update/"]
          map _size n2 @?= [1] -- actualized as there was no cache
    ]

-- * option parsing

preParseArgs' :: [String] -> ([String], [ByteString])
preParseArgs' ss = preParseArgs (ss, map B8.fromString ss)

testOptParse :: TestTree
testOptParse = testCase "option parsing tests" $ do
  shortOptions @?= "RlzkI" -- reminder to update these tests if options change
  longOptions @?= ["depth", "cache-level", "minsize", "mincount", "init-db", "files-from"]
  preParseArgs' []                          @?= ([], [])
  preParseArgs' ["x"]                       @?= ([], ["x"])
  preParseArgs' ["-"]                       @?= ([], ["-"])
  preParseArgs' [""]                        @?= ([], [""])
  preParseArgs' ["-x"]                      @?= (["-x"], []) -- non-exitant short option
  preParseArgs' ["-m"]                      @?= (["-m"], [])
  preParseArgs' ["-l1"]                     @?= (["-l1"], [])
  preParseArgs' ["-l", "1"]                 @?= (["-l", "1"], [])
  preParseArgs' ["-x", "1"]                 @?= (["-x"], ["1"])
  preParseArgs' ["-l1"]                     @?= (["-l1"], [])
  preParseArgs' ["-x1"]                     @?= (["-x1"], [])
  preParseArgs' ["-xl1"]                    @?= (["-xl1"], [])
  preParseArgs' ["-xl12"]                   @?= (["-xl12"], [])
  preParseArgs' ["-lx1"]                    @?= (["-lx1"], [])
  preParseArgs' ["-xl", "1"]                @?= (["-xl", "1"], [])
  preParseArgs' ["-lx", "1"]                @?= (["-lx"], ["1"])
  preParseArgs' ["--nothing", "1", "2"]     @?= (["--nothing"], ["1", "2"])
  preParseArgs' ["--depth", "1", "2"]       @?= (["--depth", "1"], ["2"])
  preParseArgs' ["--", "--depth", "1", "2"] @?= (["--"], ["--depth", "1", "2"])
