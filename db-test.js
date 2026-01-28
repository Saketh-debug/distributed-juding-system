// db-test.js
const { Pool } = require('pg');
const { Queue } = require('bullmq');

// 1. Test Database
const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'contest_db',
    password: 'password',
    port: 5432,
});

// 2. Test Queue
const testQueue = new Queue('test-queue', { 
    connection: { host: 'localhost', port: 6379 } 
});

async function checkSystems() {
    try {
        // DB Check
        const res = await pool.query('SELECT NOW()');
        console.log('✅ PostgreSQL is Connected! Time:', res.rows[0].now);

        // Redis Check
        await testQueue.add('test-job', { foo: 'bar' });
        console.log('✅ Redis Queue is Working! Job added.');
        
        process.exit(0);
    } catch (err) {
        console.error('❌ Connection Failed:', err);
        process.exit(1);
    }
}

checkSystems();