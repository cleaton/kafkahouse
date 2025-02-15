use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, Throughput, measurement::WallTime};
use tokio::runtime::{Runtime, Builder};
use tokio::sync::Semaphore;
use futures::future::join_all;
use kafkahouse::storage::MessageStore;
use std::time::Duration;
use std::sync::Arc;
use std::time::Instant;
use tracing_subscriber;

async fn generate_test_batch(batch_size: usize, message_size: usize) -> (String, u32, Vec<String>) {
    let data = "a".repeat(message_size);
    let messages = vec![data; batch_size];
    ("test_topic".to_string(), 0, messages)
}

fn benchmark_storage(c: &mut Criterion<WallTime>) {
    // Initialize tracing with info level
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init()
        .ok();

    let rt = Builder::new_multi_thread()
        .worker_threads(100)
        .enable_time()
        .enable_io()
        .build()
        .unwrap();

    let store = rt.block_on(async {
        MessageStore::new("http://localhost:8123").unwrap()
    });
    let store = Arc::new(store);

    let mut group = c.benchmark_group("storage");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);

    // Simulate Kafka-like workload
    let kafka_batch_sizes = [10, 50, 100]; // Typical Kafka batch sizes
    let message_size = 1000;
    let total_messages = 500;
    let concurrency_levels = [50, 100];

    println!("\nRunning storage benchmarks (simulating Kafka workload):");
    println!("- Each scenario will be tested {} times", 10);
    println!("- Message size: {} bytes", message_size);
    println!("- Total messages per test: {}", total_messages);
    println!("- Testing Kafka batch sizes: {:?}", kafka_batch_sizes);
    println!("- Testing concurrency levels: {:?}", concurrency_levels);
    println!("- Total data per test: {:.2} MB", 
             (total_messages * message_size) as f64 / 1_000_000.0);

    for &kafka_batch_size in kafka_batch_sizes.iter() {
        let batches_per_test = total_messages / kafka_batch_size;
        let test_batch = rt.block_on(generate_test_batch(kafka_batch_size, message_size));
        
        for &max_concurrent in concurrency_levels.iter() {
            println!("\nTesting Kafka batch size: {} messages with {} concurrent tasks:", 
                    kafka_batch_size, max_concurrent);
            
            let mut times = Vec::new();
            let mut all_batch_times = Vec::new();
            
            group.bench_function(
                format!("kafka_batch_{}_concurrent_{}", kafka_batch_size, max_concurrent), 
                |b| {
                    b.iter_custom(|iters| {
                        let mut total_time = Duration::ZERO;
                        for _ in 0..iters {
                            let start = Instant::now();
                            let mut individual_batch_times = Vec::new();
                            
                            rt.block_on(async {
                                let semaphore = Arc::new(Semaphore::new(max_concurrent));
                                let mut handles = Vec::with_capacity(batches_per_test);
                                
                                for i in 0..batches_per_test {
                                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                                    let store = store.clone();
                                    let batch = test_batch.clone();
                                    
                                    handles.push(tokio::spawn(async move {
                                        let _permit = permit;
                                        let batch_start = Instant::now();
                                        let result = store.append_message_batches(vec![batch]).await;
                                        let batch_time = batch_start.elapsed();
                                        (result, batch_time)
                                    }));
                                }
                                
                                let results = join_all(handles).await;
                                
                                for result in results {
                                    let (op_result, batch_time) = result.unwrap();
                                    op_result.unwrap();
                                    individual_batch_times.push(batch_time);
                                }
                            });
                            
                            let elapsed = start.elapsed();
                            total_time += elapsed;
                            times.push(elapsed);
                            all_batch_times.extend(individual_batch_times);
                        }
                        total_time
                    })
                }
            );

            if !times.is_empty() {
                times.sort();
                all_batch_times.sort();
                
                let total_time: Duration = times.iter().sum();
                let avg_time = total_time / times.len() as u32;
                let p95 = times[times.len() * 95 / 100];
                let p99 = times[times.len() * 99 / 100];
                let max_time = times.last().unwrap();
                
                let msgs_per_sec = total_messages as f64 / avg_time.as_secs_f64();
                let mb_per_sec = (total_messages * message_size) as f64 / avg_time.as_secs_f64() / 1_000_000.0;

                let avg_batch_time = all_batch_times.iter().sum::<Duration>() / all_batch_times.len() as u32;
                let p95_batch = all_batch_times[all_batch_times.len() * 95 / 100];
                let p99_batch = all_batch_times[all_batch_times.len() * 99 / 100];
                let max_batch = all_batch_times.last().unwrap();

                println!("\nResults for Kafka batch size {} with {} concurrent tasks:", 
                        kafka_batch_size, max_concurrent);
                println!("Overall test statistics:");
                println!("  Average latency: {:?}", avg_time);
                println!("  p95 latency: {:?}", p95);
                println!("  p99 latency: {:?}", p99);
                println!("  Max latency: {:?}", max_time);
                println!("  Throughput: {:.2} messages/sec", msgs_per_sec);
                println!("  Throughput: {:.2} MB/sec", mb_per_sec);
                println!("\nIndividual batch statistics:");
                println!("  Average batch time: {:?}", avg_batch_time);
                println!("  p95 batch time: {:?}", p95_batch);
                println!("  p99 batch time: {:?}", p99_batch);
                println!("  Max batch time: {:?}", max_batch);
                println!("  Concurrent efficiency: {:.2}x", 
                        avg_batch_time.as_secs_f64() / avg_time.as_secs_f64() * max_concurrent as f64);
            }
        }
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(10))
        .warm_up_time(Duration::from_secs(5))
        .noise_threshold(0.05)
        .significance_level(0.1)
        .without_plots();
    targets = benchmark_storage
}
criterion_main!(benches); 