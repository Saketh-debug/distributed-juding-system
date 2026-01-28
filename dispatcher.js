// const { Worker } = require('bullmq');
// const axios = require('axios');
// const { Pool } = require('pg');
// const { JUDGE_NODES, REDIS_CONFIG, PG_CONFIG } = require('./config');

// // NEW: Connect to the API Server as a client
// const io = require("socket.io-client");
// const socket = io("http://localhost:3000"); // Connects to server.js

// // Initialize Database Connection
// const db = new Pool(PG_CONFIG);

// // Round Robin Index Tracker
// let nodeIndex = 0;

// console.log(`👷 Dispatcher started. managing ${JUDGE_NODES.length} worker nodes.`);
// console.log(`   - Concurrency Limit: 10 jobs in parallel`);

// // The Queue Worker
// const worker = new Worker('judge-cluster', async (job) => {
//     const { submissionId, source_code, language_id } = job.data;
    
//     // ----------------------------------------------------
//     // 1. LOAD BALANCING (Round Robin)
//     // ----------------------------------------------------
//     const targetJudge = JUDGE_NODES[nodeIndex];
//     // Move index to next node (0 -> 1 -> 0 -> 1...)
//     nodeIndex = (nodeIndex + 1) % JUDGE_NODES.length; 

//     console.log(`[Job ${submissionId}] 🚀 Dispatching to -> ${targetJudge}`);

//     try {
//         // ----------------------------------------------------
//         // 2. MARK AS PROCESSING
//         // ----------------------------------------------------
//         await db.query("UPDATE submissions SET status = 'PROCESSING' WHERE id = $1", [submissionId]);

//         // ----------------------------------------------------
//         // 3. SEND TO WORKER LAPTOP (The Heavy Lifting)
//         // ----------------------------------------------------
//         // We use wait=true so the connection stays open until the result is ready.
//         const response = await axios.post(
//             `${targetJudge}/submissions?base64_encoded=false&wait=true`, 
//             {
//                 source_code: source_code,
//                 language_id: language_id,
//                 stdin: "" // Add stdin here if your problems need input
//             }, 
//             { timeout: 30000 } // 30s timeout safety
//         );

//         const result = response.data;

//         // ----------------------------------------------------
//         // 4. SAVE RESULT TO DB
//         // ----------------------------------------------------
//         // Check if Judge0 says "Accepted" (ID 3) or "Compilation Error" (ID 6), etc.
//         const finalStatus = result.status.id === 3 ? 'ACCEPTED' : 'WRONG_ANSWER'; // Simplify for now

//         await db.query(`
//             UPDATE submissions 
//             SET status = $1, stdout = $2, stderr = $3, execution_time = $4
//             WHERE id = $5
//         `, [finalStatus, result.stdout, result.stderr, result.time, submissionId]);

//         console.log(`[Job ${submissionId}] ✅ Completed on ${targetJudge} | Time: ${result.time}s`);
//         return result;

//     } catch (error) {
//         console.error(`[Job ${submissionId}] ❌ Failed on ${targetJudge}:`, error.message);
        
//         // Mark as FAILED in DB so the user knows something went wrong
//         await db.query("UPDATE submissions SET status = 'FAILED', stderr = $1 WHERE id = $2", [error.message, submissionId]);
        
//         // If you throw error here, BullMQ will retry. 
//         // For now, we swallow the error to prevent infinite retries on bad code.
//     }

// }, { 
//     connection: REDIS_CONFIG,
//     // CRITICAL: This is how many jobs run in parallel across ALL laptops.
//     // If you have 2 laptops with 4 cores each, set this to 8 or 10.
//     concurrency: 8 
// });

// dispatcher.js
const { Worker } = require('bullmq');
const axios = require('axios');
const { Pool } = require('pg');
const { JUDGE_NODES, REDIS_CONFIG, PG_CONFIG } = require('./config');

// Connect to the API Server as a client to send updates
const io = require("socket.io-client");
const socket = io("http://localhost:3000"); // Ensure this matches your server port

// Initialize Database Connection
const db = new Pool(PG_CONFIG);

// Round Robin Index Tracker
let nodeIndex = 0;

console.log(`👷 Dispatcher started. managing ${JUDGE_NODES.length} worker nodes.`);
console.log(`   - Concurrency Limit: 8 jobs in parallel`);

// The Queue Worker
const worker = new Worker('judge-cluster', async (job) => {
    // UPDATED: Now we also unpack 'user_id' from the job data
    const { submissionId, source_code, language_id, user_id } = job.data;
    
    // 1. LOAD BALANCING (Round Robin)
    const targetJudge = JUDGE_NODES[nodeIndex];
    nodeIndex = (nodeIndex + 1) % JUDGE_NODES.length; 

    console.log(`[Job ${submissionId}] 🚀 Dispatching to -> ${targetJudge}`);

    try {
        // 2. MARK AS PROCESSING
        await db.query("UPDATE submissions SET status = 'PROCESSING' WHERE id = $1", [submissionId]);

        // 3. SEND TO WORKER LAPTOP
        const response = await axios.post(
            `${targetJudge}/submissions?base64_encoded=false&wait=true`, 
            {
                source_code: source_code,
                language_id: language_id,
                stdin: "" 
            }, 
            { timeout: 30000 }
        );

        const result = response.data;

        // Determine Status (Simplified logic)
        // ID 3 = Accepted. Anything else (like 6 for compilation error) is treated as Wrong Answer/Error for now
        const finalStatus = result.status.id === 3 ? 'ACCEPTED' : 'WRONG_ANSWER'; 

        // 4. SAVE RESULT TO DB
        await db.query(`
            UPDATE submissions 
            SET status = $1, stdout = $2, stderr = $3, execution_time = $4
            WHERE id = $5
        `, [finalStatus, result.stdout, result.stderr, result.time, submissionId]);

        console.log(`[Job ${submissionId}] ✅ Completed on ${targetJudge}`);

        // ----------------------------------------------------
        // NEW: NOTIFY THE MAIN SERVER
        // ----------------------------------------------------
        socket.emit('internal_job_finished', {
            userId: user_id,           // Who to notify
            submissionId: submissionId,
            status: finalStatus,
            stdout: result.stdout,
            time: result.time
        });
        // ----------------------------------------------------

        return result;

    } catch (error) {
        console.error(`[Job ${submissionId}] ❌ Failed on ${targetJudge}:`, error.message);
        
        await db.query("UPDATE submissions SET status = 'FAILED', stderr = $1 WHERE id = $2", [error.message, submissionId]);
        
        // Notify server of failure so the frontend doesn't hang forever
        socket.emit('internal_job_finished', {
            userId: user_id,
            submissionId: submissionId,
            status: 'FAILED',
            error: error.message
        });
    }

}, { 
    connection: REDIS_CONFIG,
    concurrency: 8 
});