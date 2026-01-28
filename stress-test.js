// stress-test.js
const axios = require('axios');

const TOTAL_REQUESTS = 200;
const API_URL = "http://localhost:3000/submit";

// A simple Python "Hello World" to keep it light
const PAYLOAD = {
    user_id: "stress_tester",
    problem_id: "test_01",
    language_id: 71, // Python
    source_code: "print('Stress Test Execution')"
};

async function runTest() {
    console.log(`🔥 STARTING STRESS TEST: ${TOTAL_REQUESTS} REQUESTS 🔥`);
    console.log("------------------------------------------------");

    const promises = [];
    const startTime = Date.now();

    for (let i = 0; i < TOTAL_REQUESTS; i++) {
        // We push the request promise into an array to run them in PARALLEL
        const p = axios.post(API_URL, PAYLOAD)
            .then(res => process.stdout.write(".")) // Print a dot for every success
            .catch(err => process.stdout.write("X")); // Print X for error
        
        promises.push(p);
    }

    // Wait for all HTTP requests to be accepted by the API
    await Promise.all(promises);

    const duration = (Date.now() - startTime) / 1000;
    console.log(`\n\n✅ All ${TOTAL_REQUESTS} requests sent to Queue in ${duration} seconds.`);
    console.log("Check your 'dispatcher.js' terminal to see the load balancing!");
}

runTest();