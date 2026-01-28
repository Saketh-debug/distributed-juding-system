// config.js
module.exports = {
    // ----------------------------------------------------
    // 1. WORKER CONFIGURATION (The Muscle)
    // ----------------------------------------------------
    // Update these IPs whenever your laptops change networks!
    JUDGE_NODES: [
        'http://192.168.2.126:2358', // Laptop 1
        'http://192.168.2.129:2358', // Laptop 2
        // 'http://192.168.1.51:2358'  // Laptop 3 (Add more if you have them)
    ],

    // ----------------------------------------------------
    // 2. INFRASTRUCTURE CONFIGURATION (The Brain)
    // ----------------------------------------------------
    REDIS_CONFIG: {
        host: 'localhost', 
        port: 6379
    },
    
    PG_CONFIG: {
        user: 'postgres',
        host: 'localhost',
        database: 'contest_db',
        password: 'password', // Matches the docker run command from Phase 2
        port: 5432,
    }
};