const { Client } = require('ssh2');
const fs = require('fs');

const conn = new Client();
const patchContent = fs.readFileSync('C:\\Users\\Administrator\\Desktop\\free-washer\\patch_v53.py', 'utf8');

conn.on('ready', () => {
    console.log('SSH connected');
    // Upload patch file
    conn.sftp((err, sftp) => {
        if (err) { console.error('SFTP error:', err); conn.end(); return; }
        const ws = sftp.createWriteStream('/tmp/patch_v53.py', { mode: 0o755 });
        ws.on('close', () => {
            console.log('Patch uploaded to /tmp/patch_v53.py');
            // Run the patch
            conn.exec('python3 /tmp/patch_v53.py', (err, stream) => {
                if (err) { console.error('Exec error:', err); conn.end(); return; }
                let out = '';
                stream.on('data', (data) => { out += data.toString(); process.stdout.write(data); });
                stream.stderr.on('data', (data) => { process.stderr.write(data); });
                stream.on('close', (code) => {
                    console.log(`\nExit code: ${code}`);
                    // Also rebuild docker
                    if (code === 0) {
                        conn.exec('cd ~/traffic-washer && docker build -t traffic-washer:latest . 2>&1 | tail -5 && docker stop traffic-washer && docker rm traffic-washer && docker run -d --name traffic-washer --restart always -p 9999:9999 traffic-washer:latest && echo "DEPLOYED"', (err2, stream2) => {
                            if (err2) { console.error(err2); conn.end(); return; }
                            stream2.on('data', (data) => process.stdout.write(data));
                            stream2.stderr.on('data', (data) => process.stderr.write(data));
                            stream2.on('close', () => { console.log('\nDone!'); conn.end(); });
                        });
                    } else {
                        conn.end();
                    }
                });
            });
        });
        ws.write(patchContent);
        ws.end();
    });
}).on('error', (err) => {
    console.error('SSH error:', err.message);
}).connect({
    host: '192.168.2.53',
    port: 22,
    username: 'ghss',
    password: '110120'
});
