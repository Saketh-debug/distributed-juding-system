const { Worker } = require('bullmq');
const axios = require('axios');
const { Pool } = require('pg');
const { JUDGE_NODES, REDIS_CONFIG, PG_CONFIG } = require('./config');

// Connect to the API Server as a client to send updates
const io = require("socket.io-client");
const socket = io("http://localhost:3100"); 

// Initialize Database Connection
const db = new Pool(PG_CONFIG);

// Round Robin Index Tracker
let nodeIndex = 0;

console.log(`👷 Dispatcher started. managing ${JUDGE_NODES.length} worker nodes.`);
console.log(`   - Concurrency Limit: 20 jobs in parallel`);

// The Queue Worker
const worker = new Worker('judge-cluster', async (job) => {
    // Unpack job data
    const { submissionId, source_code, language_id, user_id, stdin } = job.data;
    
    // 1. LOAD BALANCING (Round Robin)
    const targetJudge = JUDGE_NODES[nodeIndex];
    nodeIndex = (nodeIndex + 1) % JUDGE_NODES.length; 

    console.log(`[Job ${submissionId}] 🚀 Dispatching to -> ${targetJudge}`);

    try {
        // 2. MARK AS PROCESSING
        await db.query("UPDATE submissions SET status = 'PROCESSING' WHERE id = $1", [submissionId]);

        // 3. SEND TO WORKER LAPTOP (CORRECTED PAYLOAD)
        const payload = {
            source_code: source_code,
            language_id: parseInt(language_id), // Ensure integer
            stdin: stdin || "",                 // Ensure string
            base64_encoded: false               // <--- CRITICAL FIX: Add this to body
        };

        const response = await axios.post(
            `${targetJudge}/submissions?wait=true`, // URL
            payload,                                // Body
            { timeout: 30000 }                      // Config
        );

        const result = response.data;

        // Determine Status
        // Judge0 ID 3 = Accepted.
        const finalStatus = result.status.id === 3 ? 'ACCEPTED' : 'WRONG_ANSWER'; 
        // Note: You might want to map other IDs (like 6 for Compilation Error) differently later.

        // 4. SAVE RESULT TO DB
        await db.query(`
            UPDATE submissions 
            SET status = $1, stdout = $2, stderr = $3, execution_time = $4
            WHERE id = $5
        `, [finalStatus, result.stdout || "", result.stderr || "", result.time || 0, submissionId]);

        console.log(`[Job ${submissionId}] ✅ Completed on ${targetJudge}`);

        // 5. NOTIFY THE MAIN SERVER
        socket.emit('internal_job_finished', {
            userId: user_id,
            submissionId: submissionId,
            status: finalStatus,
            stdout: result.stdout || "",
            stderr: result.stderr || "",
            time: result.time
        });

        return result;

    } catch (error) {
        // Safe error handling to prevent crash
        const errorMsg = error.response?.data ? JSON.stringify(error.response.data) : error.message;
        console.error(`[Job ${submissionId}] ❌ Failed on ${targetJudge}:`, errorMsg);
        
        await db.query("UPDATE submissions SET status = 'FAILED', stderr = $1 WHERE id = $2", [errorMsg, submissionId]);
        
        socket.emit('internal_job_finished', {
            userId: user_id,
            submissionId: submissionId,
            status: 'FAILED',
            error: errorMsg
        });
    }

}, { 
    connection: REDIS_CONFIG,
    concurrency: 20
});