{-# LANGUAGE CPP , OverloadedStrings #-}
module Main where
#  ifdef ASSERTIONS_ON
#    error "Sorry, please reconfigure without -finstrumented so that we turn off assertions in library code."
#  endif

import Criterion.Main
import Control.DeepSeq
import Control.Monad
import Control.Concurrent
import qualified Data.Text as T

import Control.Concurrent.BloomFilter.Internal
import qualified Control.Concurrent.BloomFilter as Bloom
import Data.Hashabler

import qualified Data.Set as Set
import qualified Data.HashSet as HashSet

-- import System.IO.Unsafe(unsafePerformIO)
import System.Random

-- TODO comparisons with:
--   - pure Set
--   - best in class Int (or other specialized) hash map or trie
--   - general hashmap (of Hashable things)
--   - the above, wrapped in an IORef or MVar

main :: IO ()
main = do
    assertionsOn <- assertionCanary
    when assertionsOn $
      putStrLn  $ "!!! WARNING !!! assertions are enabled in library code and may result in "
                ++"slower than realistic benchmarks. Try configuring without -finstrumented"

    procs <- getNumCapabilities
    if procs < 2 
        then putStrLn "!!! WARNING !!!: Some benchmarks are only valid if more than 1 core is available"
        else return ()
    

    -- TODO make this a function will call in 'env'
    let g = mkStdGen 8973459
        chars = randoms g :: [Char]
        fakeWords = go chars
        go :: [Char] -> [String]
        go [] = error "noninfinite list"
        go (s:ss) = let (a,as) = splitAt 3 ss
                        (b,bs) = splitAt 5 as
                        (c,cs) = splitAt 5 bs
                        (d,ds) = splitAt 6 cs
                        (e,es) = splitAt 8 ds
                     in [s]:a:b:c:d:e:(go es)

    let textWords10k = map T.pack $ take 10000 fakeWords
        (wds5k_0, wds5k_1) = splitAt 5000 textWords10k
    deepseq textWords10k $ return ()

    let txt = "orange" :: T.Text
        -- so half are in set and half are not:
        txt10New, txt10Mix :: [T.Text]
        txt10New = take 10 $ reverse textWords10k
        txt10Mix = concatMap (\(x,y)->[x,y]) $ zip textWords10k (take 5 txt10New)
    


    defaultMain [
      bgroup "internals" [
          env (Bloom.new (SipKey 1 1) 5 20) $ \ ~b->
            bench "membershipWordAndBits64" $ nf (membershipWordAndBits64 (Hash64 1)) b
        , env (Bloom.new (SipKey 1 1) 13 20) $ \ ~b->
            bench "membershipWordAndBits128" $ nf (membershipWordAndBits128 (Hash128 1 1)) b
        ],

      -- For comparing cache behavior with perf, against below:
      bgroup "HashSet" $
        [ bench "10K insert" $ whnf (HashSet.fromList) textWords10k ],
      bgroup "Set" $
        [ bench "10K insert" $ whnf (Set.fromList) textWords10k ],

      bgroup "different sizes" $
        let benches b = [
                bench "10K inserts" $ whnfIO $ manyInserts b textWords10k
              , bench "10K lookups" $ whnfIO $ manyLookups b textWords10k
              ]
         in
            [ env (Bloom.new (SipKey 11 22) 3 12) $ \ ~b -> 
                bgroup "4096" (benches b)
            , env (Bloom.new (SipKey 11 22) 3 14) $ \ ~b -> 
                bgroup "16384" (benches b)
            , env (Bloom.new (SipKey 11 22) 3 16) $ \ ~b -> 
                bgroup "65536" (benches b)
            , env (Bloom.new (SipKey 11 22) 3 20) $ \ ~b -> 
                bgroup "1MB" (benches b)
            , env (Bloom.new (SipKey 11 22) 3 24) $ \ ~b -> 
                bgroup "8MB" (benches b)
            , env (Bloom.new (SipKey 11 22) 3 27) $ \ ~b -> 
                bgroup "64MB" (benches b)
            ]
      , bgroup "different sizes (concurrency)" $
        {-
          -- TODO factor out cost of 'new' in some better way:
        [ env (Bloom.new (SipKey 11 22) 3 12) $ \ ~b -> 
            bench "bigInsertLookup 15k ops" $  whnfIO (largeInsertQueryBench b wds5k_0 wds5k_1)

        , env (Bloom.new (SipKey 11 22) 3 12) $ \ ~b -> 
           bench "bigInsertLookup 15k ops across two threads (4096)" $ whnfIO (largeInsertQueryBenchTwoThreads b 5000 wds5k_0 wds5k_1)
        , env (Bloom.new (SipKey 11 22) 3 14) $ \ ~b -> 
           bench "bigInsertLookup 15k ops across two threads (16384)" $ whnfIO (largeInsertQueryBenchTwoThreads b 5000 wds5k_0 wds5k_1)
        , env (Bloom.new (SipKey 11 22) 3 16) $ \ ~b -> 
           bench "bigInsertLookup 15k ops across two threads (65536)" $ whnfIO (largeInsertQueryBenchTwoThreads b 5000 wds5k_0 wds5k_1)
        , env (Bloom.new (SipKey 11 22) 3 20) $ \ ~b -> 
           bench "bigInsertLookup 15k ops across two threads (1MB)" $ whnfIO (largeInsertQueryBenchTwoThreads b 5000 wds5k_0 wds5k_1)
        , env (Bloom.new (SipKey 11 22) 3 24) $ \ ~b -> 
           bench "bigInsertLookup 15k ops across two threads (8MB)" $ whnfIO (largeInsertQueryBenchTwoThreads b 5000 wds5k_0 wds5k_1)
        , env (Bloom.new (SipKey 11 22) 3 27) $ \ ~b -> 
           bench "bigInsertLookup 15k ops across two threads (64MB)" $ whnfIO (largeInsertQueryBenchTwoThreads b 5000 wds5k_0 wds5k_1)
        -}
        let benches b = [
                bench "10K inserts, across 2 threads" $ whnfIO $ manyInsertsTwoThreads b wds5k_0 wds5k_1
              , bench "10K lookups, across 2 threads" $ whnfIO $ manyLookupsTwoThreads b wds5k_0 wds5k_1
              ]
         in
            [ env (Bloom.new (SipKey 11 22) 3 12) $ \ ~b -> 
                bgroup "4096" (benches b)
            , env (Bloom.new (SipKey 11 22) 3 14) $ \ ~b -> 
                bgroup "16384" (benches b)
            , env (Bloom.new (SipKey 11 22) 3 16) $ \ ~b -> 
                bgroup "65536" (benches b)
            , env (Bloom.new (SipKey 11 22) 3 20) $ \ ~b -> 
                bgroup "1MB" (benches b)
            , env (Bloom.new (SipKey 11 22) 3 24) $ \ ~b -> 
                bgroup "8MB" (benches b)
            , env (Bloom.new (SipKey 11 22) 3 27) $ \ ~b -> 
                bgroup "64MB" (benches b)
            ]
      , bgroup "lookup insert" [
          bgroup "Int" [
              bench "siphash64_1_3 for comparison" $ whnf (siphash64_1_3 (SipKey 1 1)) (1::Int)
            , env (Bloom.new (SipKey 1 1) 3 12) $ \ ~b->
              bgroup "3 12 (64-bit hash)" [

                  -- best case, with no cache effects (I think):
                  bench "lookup x1" $ whnfIO (Bloom.lookup b (1::Int))
                , bench "lookup x10" $ nfIO (mapM_ (Bloom.lookup b) [1..10])
                , bench "lookup x100" $ nfIO (mapM_ (Bloom.lookup b) [1..100])

                , bench "insert x1" $ whnfIO (Bloom.insert b (1::Int))
                , bench "insert x10" $ nfIO (mapM_ (Bloom.insert b) [1..10])
                , bench "insert x100" $ nfIO (mapM_ (Bloom.insert b) [1..100])
              ]
            , env (Bloom.new (SipKey 1 1) 5 20) $ \ ~b->
              bgroup "5 20 (64-bit hash)" [

                  -- best case, with no cache effects (I think):
                  bench "lookup x1" $ whnfIO (Bloom.lookup b (1::Int))
                , bench "lookup x10" $ nfIO (mapM_ (Bloom.lookup b) [1..10])
                , bench "lookup x100" $ nfIO (mapM_ (Bloom.lookup b) [1..100])

                , bench "insert x1" $ whnfIO (Bloom.insert b (1::Int))
                , bench "insert x10" $ nfIO (mapM_ (Bloom.insert b) [1..10])
                , bench "insert x100" $ nfIO (mapM_ (Bloom.insert b) [1..100])
              ]
            , env (Bloom.new (SipKey 1 1) 13 20) $ \ ~b->
              bgroup "13 20 (128-bit hash)" [

                  bench "lookup x1" $ whnfIO (Bloom.lookup b (1::Int))
                , bench "lookup x10" $ nfIO (mapM_ (Bloom.lookup b) [1..10])
                , bench "lookup x100" $ nfIO (mapM_ (Bloom.lookup b) [1..100])

                , bench "insert x1" $ whnfIO (Bloom.insert b (1::Int))
                , bench "insert x10" $ nfIO (mapM_ (Bloom.insert b) [1..10])
                , bench "insert x100" $ nfIO (mapM_ (Bloom.insert b) [1..100])
              ]
          ],
          bgroup "Text" [
              bench "siphash64_1_3 for comparison" $ whnf (siphash64_1_3 (SipKey 1 1)) (txt)
            , env (Bloom.new (SipKey 1 1) 3 12) $ \ ~b->
              bgroup "3 12 (64-bit hash)" [

                  -- best case, with no cache effects (I think):
                  bench "lookup x1" $ whnfIO (Bloom.lookup b txt)
                , bench "lookup x10" $ nfIO (mapM_ (Bloom.lookup b) (take 10 textWords10k))
                , bench "lookup x100" $ nfIO (mapM_ (Bloom.lookup b) (take 100 textWords10k))

                , bench "insert x1" $ whnfIO (Bloom.insert b txt)
                , bench "insert x10" $ nfIO (mapM_ (Bloom.insert b) (take 10 textWords10k))
                , bench "insert x100" $ nfIO (mapM_ (Bloom.insert b) (take 100 textWords10k))
              ]
            , env (Bloom.new (SipKey 1 1) 5 20) $ \ ~b->
              bgroup "5 20 (64-bit hash)" [

                  -- best case, with no cache effects (I think):
                  bench "lookup x1" $ whnfIO (Bloom.lookup b txt)
                , bench "lookup x10" $ nfIO (mapM_ (Bloom.lookup b) (take 10 textWords10k))
                , bench "lookup x100" $ nfIO (mapM_ (Bloom.lookup b) (take 100 textWords10k))

                , bench "insert x1" $ whnfIO (Bloom.insert b txt)
                , bench "insert x10" $ nfIO (mapM_ (Bloom.insert b) (take 10 textWords10k))
                , bench "insert x100" $ nfIO (mapM_ (Bloom.insert b) (take 100 textWords10k))
              ]
            , env (Bloom.new (SipKey 1 1) 13 20) $ \ ~b->
              bgroup "13 20 (128-bit hash)" [

                  bench "lookup x1" $ whnfIO (Bloom.lookup b txt)
                , bench "lookup x10" $ nfIO (mapM_ (Bloom.lookup b) (take 10 textWords10k))
                , bench "lookup x100" $ nfIO (mapM_ (Bloom.lookup b) (take 100 textWords10k))

                , bench "insert x1" $ whnfIO (Bloom.insert b txt)
                , bench "insert x10" $ nfIO (mapM_ (Bloom.insert b) (take 10 textWords10k))
                , bench "insert x100" $ nfIO (mapM_ (Bloom.insert b) (take 100 textWords10k))
              ]
          ]

        ],
  --
  -- TODO check  TO SEE HOW THINGS LOOK BEFORE AND AFTER UNFOLDING CHANGE,
  --             MAYBE TRY DOING inserts/lookups x10 here.
  --   3x12 insert went from  51.8 to 49  (below)
  --   5x20 insert went from  59.1 to 47.6 (in "lookup insert")
      bgroup "comparisons micro x1 " [
          bench "(just siphash64_1_3 on txt for below)" $ whnf (siphash64_1_3 (SipKey 1 1)) ("orange"::T.Text)
        -- This has 0.3% fpr for 10000 elements, so I think can be fairly compared
        , env (Bloom.new (SipKey 11 22) 3 12) $ \ ~b_text->
          bgroup "unagi-bloomfilter 3 12" [
              bench "insert" $ whnfIO (Bloom.insert b_text txt)
            {- I was concerned that the above might not be valid (perhaps the
             - hashing of the Text value was getting reused?), but the following
             - convinced me it's all right; we can see differences in size of input
             - string reflected in all these benchmarks. I believe bloomInsertPure1
             - reflects the inability to inline Hashable instance machinery (since
             - it must remain polymorphic.
            , bench "Bloom.insert (64)(validation1)" $ whnf (bloomInsertPure1 b_text) txt
            , bench "Bloom.insert (64)(validation2)" $ whnf (bloomInsertPure2 b_text) txt
            , bench "Bloom.insert (64)(validation3)" $ whnfIO (Bloom.insert b_text "ora")
            , bench "Bloom.insert (64)(validation4)" $ whnf (bloomInsertPure1 b_text) "ora"
            , bench "Bloom.insert (64)(validation5)" $ whnf (bloomInsertPure2 b_text) "ora"
            , bench "(validation orange)" $ whnf (siphash64_1_3 (SipKey 1 1)) ("orange"::T.Text)
            , bench "(validation ora)" $ whnf (siphash64_1_3 (SipKey 1 1)) ("ora"::T.Text)
            -}
            , bench "lookup" $ nfIO (Bloom.lookup b_text txt)
          ]

        , env (return $ HashSet.fromList $ take 10 textWords10k) $ \ ~hashset10->
          bgroup "HashSet Text (10)" [
              bench "insert" $ whnf (\t-> HashSet.insert t hashset10) txt
            , bench "member" $ nf (\t-> HashSet.member t hashset10) txt
          ]
        , env (return $ HashSet.fromList $ take 100 textWords10k) $ \ ~hashset100->
          bgroup "HashSet Text (100)" [
              bench "insert" $ whnf (\t-> HashSet.insert t hashset100) txt
            , bench "member" $ nf (\t-> HashSet.member t hashset100) txt
          ]
        , env (return $ HashSet.fromList $ take 10000 textWords10k) $ \ ~hashset10000->
          bgroup "HashSet Text (10000)" [
              bench "insert" $ whnf (\t-> HashSet.insert t hashset10000) txt
            , bench "member" $ nf (\t-> HashSet.member t hashset10000) txt
          ]
        , env (return $ Set.fromList $ take 10 textWords10k) $ \ ~set10->
          bgroup "Set Text (10)" [
              bench "insert" $ whnf (\t-> Set.insert t set10) txt
            , bench "member" $ nf (\t-> Set.member t set10) txt
          ]
        , env (return $ Set.fromList $ take 100 textWords10k) $ \ ~set100->
          bgroup "Set Text (100)" [
              bench "insert" $ whnf (\t-> Set.insert t set100) txt
            , bench "member" $ nf (\t-> Set.member t set100) txt
          ]
        , env (return $ Set.fromList $ take 10000 textWords10k) $ \ ~set10000->
          bgroup "Set Text (10000)" [
              bench "insert" $ whnf (\t-> Set.insert t set10000) txt
            , bench "member" $ nf (\t-> Set.member t set10000) txt
          ]
      ],
      
      bgroup "comparisons micro x10" [
        -- This has 0.3% fpr for 10000 elements, so I think can be fairly compared
          env (Bloom.new (SipKey 11 22) 3 12) $ \ ~b_text->
          bgroup "unagi-bloomfilter 3 12" [
              bench "insert" $ whnfIO (mapM (Bloom.insert b_text) txt10New)
            , bench "lookup" $ nfIO (mapM (Bloom.lookup b_text) txt10Mix)
          ]

        , env (return $ HashSet.fromList $ take 100 textWords10k) $ \ ~hashset100->
          bgroup "HashSet Text (100)" [
              bench "insert" $ whnf (foldr (\t s-> HashSet.insert t s) hashset100) txt10New
            , bench "member" $ nf (map $ \t-> HashSet.member t hashset100) txt10Mix
          ]
        , env (return $ HashSet.fromList $ take 10000 textWords10k) $ \ ~hashset10000->
          bgroup "HashSet Text (10000)" [
              bench "insert" $ whnf (foldr (\t s-> HashSet.insert t s) hashset10000) txt10New
            , bench "member" $ nf (map $ \t-> HashSet.member t hashset10000) txt10Mix
          ]
        , env (return $ Set.fromList $ take 100 textWords10k) $ \ ~set100->
          bgroup "Set Text (100)" [
              bench "insert" $ whnf (foldr (\t s-> Set.insert t s) set100) txt10New
            , bench "member" $ nf (map $ \t-> Set.member t set100) txt10Mix
          ]
        , env (return $ Set.fromList $ take 10000 textWords10k) $ \ ~set10000->
          bgroup "Set Text (10000)" [
              bench "insert" $ whnf (foldr (\t s-> Set.insert t s) set10000) txt10New
            , bench "member" $ nf (map $ \t-> Set.member t set10000) txt10Mix
          ]
      ],

      bgroup "comparisons big" [
          -- TODO large random lookup and insert benchmark, comparing with single-thread and then with work split.
          --      make this how we compare as well?
          --      Do this for various types of elements
      ],

      bgroup "combining and creation" [
        -- These timings can be subtracted from union timings:
          bench "new 14" $ whnfIO $ Bloom.new (SipKey 1 1) 3 14
        , bench "new 20" $ whnfIO $ Bloom.new (SipKey 1 1) 3 20

        , bench "unionInto (14 -> 14)" $ whnfIO $ unionBench 14 14
        , bench "unionInto (20 -> 14)" $ whnfIO $ unionBench 20 14 -- 20 is 6x
        , bench "unionInto (20 -> 20)" $ whnfIO $ unionBench 20 20
        ]
      ]

unionBench :: Int -> Int -> IO ()
unionBench bigl littlel = do
    b1 <- Bloom.new (SipKey 1 1) 3 bigl
    b2 <- Bloom.new (SipKey 1 1) 3 littlel
    b1 `Bloom.unionInto` b2


instance NFData (BloomFilter a) where
  rnf _ = ()

{-
-- TODO fix both of these and compare with Set/HashSet (wrapped in IORef or MVar for second)
largeInsertQueryBench :: Bloom.BloomFilter T.Text -> [T.Text] -> [T.Text] -> IO ()
largeInsertQueryBench b payload antipayload = do
  forM_ payload $ Bloom.insert b
  forM_ (zip payload antipayload) $ \(x,y)-> do
    --- can't test, since we're re-using bloom:
    _xOk <- Bloom.lookup b x
    _yOk <- Bloom.lookup b y  -- usually False
    -- unless (xOk) $ error "largeInsertQueryBench"
    return ()

largeInsertQueryBenchTwoThreads :: Bloom.BloomFilter T.Text -> Int -> [T.Text] -> [T.Text] -> IO ()
largeInsertQueryBenchTwoThreads b length_payload payload antipayload = do
  t0 <- newEmptyMVar
  t1 <- newEmptyMVar
  let (payload0,payload1) = splitAt (length_payload `div` 2) payload
  let (antipayload0,antipayload1) = splitAt (length_payload `div` 2) antipayload

  let go pld antpld v = do
          forM_ pld $ Bloom.insert b
          forM_ (zip pld antpld) $ \(x,y)-> do
            _xOk <- Bloom.lookup b x
            _yOk <- Bloom.lookup b y  -- usually False
            -- unless (xOk) $ error "largeInsertQueryBench"
            return ()
          putMVar v ()
  void $ forkIO $ go payload0 antipayload0 t0
  void $ forkIO $ go payload1 antipayload1 t1
  takeMVar t0 >> takeMVar t1
  -}


-- These are mostly to check cache behavior, and I don't expect it to matter
-- whether a bloom filter was already "filled with elements" or not.
manyInserts :: Bloom.BloomFilter T.Text -> [T.Text] -> IO ()
manyInserts b payload = do
  forM_ payload (void . Bloom.insert b)

manyLookups :: Bloom.BloomFilter T.Text -> [T.Text] -> IO ()
manyLookups b payload = do
  forM_ payload (void . Bloom.lookup b)

manyInsertsTwoThreads :: Bloom.BloomFilter T.Text -> [T.Text] -> [T.Text] -> IO ()
manyInsertsTwoThreads b payload0 payload1 = do
  t0 <- newEmptyMVar
  t1 <- newEmptyMVar
  let go pld v = manyInserts b pld >> putMVar v ()
  void $ forkIO $ go payload0 t0
  void $ forkIO $ go payload1 t1
  takeMVar t0 >> takeMVar t1

manyLookupsTwoThreads :: Bloom.BloomFilter T.Text -> [T.Text] -> [T.Text] -> IO ()
manyLookupsTwoThreads b payload0 payload1 = do
  t0 <- newEmptyMVar
  t1 <- newEmptyMVar
  let go pld v = manyLookups b pld >> putMVar v ()
  void $ forkIO $ go payload0 t0
  void $ forkIO $ go payload1 t1
  takeMVar t0 >> takeMVar t1


{-
-- So we can use whnf, and make sure hashes aren't being cached
{-# NOINLINE bloomInsertPure1 #-}
bloomInsertPure1 :: Hashable a => BloomFilter a -> a -> Bool
bloomInsertPure1 b = unsafePerformIO . Bloom.insert b

bloomInsertPure2 :: Hashable a => BloomFilter a -> a -> Bool
bloomInsertPure2 b = unsafePerformIO . Bloom.insert b

{-# NOINLINE bloomInsertPure3 #-}
bloomInsertPure3 :: BloomFilter Text -> Text -> Bool
bloomInsertPure3 b = unsafePerformIO . Bloom.insert b
-}
