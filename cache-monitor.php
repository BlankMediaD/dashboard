<?php
// Redis Cache Monitor for Tuesday Dashboard
// This script provides statistics about Redis cache usage

// Redis configuration - must match the settings in data.php
$redisHost = 'localhost';
$redisPort = 6379;
$redisPassword = null; // Set this if your Redis server requires authentication
$redisTimeout = 2.5;

// Initialize Redis connection
try {
    $redis = new Redis();
    $connected = $redis->connect($redisHost, $redisPort, $redisTimeout);
    if ($connected && $redisPassword) {
        $redis->auth($redisPassword);
    }
    if (!$connected) {
        die("Redis connection failed");
    }
} catch (Exception $e) {
    die("Redis error: " . $e->getMessage());
}

// Get Redis info
$info = $redis->info();
$dbSize = $redis->dbSize();
$tuesdayKeys = $redis->keys('tuesday_*');
$totalTuesdayKeys = count($tuesdayKeys);

// Get memory usage
$usedMemory = formatBytes($info['used_memory']);
$usedMemoryPeak = formatBytes($info['used_memory_peak']);

// Get hit/miss ratio
$keyspaceHits = isset($info['keyspace_hits']) ? $info['keyspace_hits'] : 0;
$keyspaceMisses = isset($info['keyspace_misses']) ? $info['keyspace_misses'] : 0;
$totalRequests = $keyspaceHits + $keyspaceMisses;
$hitRatio = $totalRequests > 0 ? round(($keyspaceHits / $totalRequests) * 100, 2) : 0;

// Format bytes to human-readable format
function formatBytes($bytes, $precision = 2) {
    $units = ['B', 'KB', 'MB', 'GB', 'TB'];
    $bytes = max($bytes, 0);
    $pow = floor(($bytes ? log($bytes) : 0) / log(1024));
    $pow = min($pow, count($units) - 1);
    $bytes /= (1 << (10 * $pow));
    return round($bytes, $precision) . ' ' . $units[$pow];
}

// Get key types and expiry times
$keyDetails = [];
foreach ($tuesdayKeys as $key) {
    $type = $redis->type($key);
    $ttl = $redis->ttl($key);
    $size = 0;
    
    // Get size based on type
    if ($type == Redis::REDIS_STRING) {
        $value = $redis->get($key);
        $size = strlen($value);
    } elseif ($type == Redis::REDIS_HASH) {
        $size = $redis->hLen($key);
    } elseif ($type == Redis::REDIS_LIST) {
        $size = $redis->lLen($key);
    } elseif ($type == Redis::REDIS_SET) {
        $size = $redis->sCard($key);
    } elseif ($type == Redis::REDIS_ZSET) {
        $size = $redis->zCard($key);
    }
    
    // Calculate creation date based on TTL and expiry time
    $creationDate = 'Unknown';
    if ($ttl > 0) {
        $creationTimestamp = time() - (2592000 - $ttl);
        $creationDate = date('Y-m-d H:i:s', $creationTimestamp);
    }
    
    $keyDetails[] = [
        'key' => $key,
        'type' => getTypeName($type),
        'ttl' => $ttl,
        'expires_in' => formatTTL($ttl),
        'size' => formatBytes($size),
        'created_at' => $creationDate
    ];
}

// Get type name
function getTypeName($typeCode) {
    switch ($typeCode) {
        case Redis::REDIS_STRING: return 'String';
        case Redis::REDIS_HASH: return 'Hash';
        case Redis::REDIS_LIST: return 'List';
        case Redis::REDIS_SET: return 'Set';
        case Redis::REDIS_ZSET: return 'Sorted Set';
        default: return 'Unknown';
    }
}

// Format TTL
function formatTTL($ttl) {
    if ($ttl < 0) {
        return 'No expiry';
    }
    
    if ($ttl < 60) {
        return "$ttl seconds";
    }
    
    if ($ttl < 3600) {
        $minutes = floor($ttl / 60);
        $seconds = $ttl % 60;
        return "$minutes min, $seconds sec";
    }
    
    if ($ttl < 86400) {
        $hours = floor($ttl / 3600);
        $minutes = floor(($ttl % 3600) / 60);
        return "$hours hr, $minutes min";
    }
    
    // For values close to 30 days (2592000 seconds), show as 30 days
    if ($ttl >= 2500000 && $ttl <= 2600000) {
        return "30 days";
    }
    
    $days = floor($ttl / 86400);
    $hours = floor(($ttl % 86400) / 3600);
    return "$days days, $hours hr";
}

// Handle cache flush action
if (isset($_POST['flush_tuesday'])) {
    foreach ($tuesdayKeys as $key) {
        $redis->del($key);
    }
    header('Location: ' . $_SERVER['PHP_SELF']);
    exit;
}

// Handle delete key action
if (isset($_POST['delete_key'])) {
    $keyToDelete = $_POST['delete_key'];
    $redis->del($keyToDelete);
    header('Location: ' . $_SERVER['PHP_SELF']);
    exit;
}

// Handle sorting
$sortField = isset($_GET['sort']) ? $_GET['sort'] : 'key';
$sortOrder = isset($_GET['order']) ? $_GET['order'] : 'asc';

// Validate sort field
$validSortFields = ['key', 'type', 'size', 'expires_in', 'created_at'];
if (!in_array($sortField, $validSortFields)) {
    $sortField = 'key';
}

// Sort the key details array
usort($keyDetails, function($a, $b) use ($sortField, $sortOrder) {
    if ($sortOrder === 'asc') {
        return $a[$sortField] <=> $b[$sortField];
    } else {
        return $b[$sortField] <=> $a[$sortField];
    }
});

// HTML output
?>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Redis Cache Monitor - Tuesday Dashboard</title>
    <style>
        :root {
            --primary-color: #4a5d7e;
            --secondary-color: #f0f2f5;
            --text-color: #2c3e50;
            --border-color: #d1d9e6;
            --hover-color: #e6eaf0;
            --success-color: #28a745;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f7fa;
            color: var(--text-color);
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        
        .header {
            font-size: 24px;
            font-weight: 600;
            margin: 0;
            padding-top: 20px;
            padding-bottom: 10px;
        }
        
        h1, h2, h3 {
            color: var(--text-color);
        }
        
        .card-header{
            font-size: 18px;
            font-weight: 500;
            margin: 0;
        }
        
        .card {
            background-color: #fff;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
            padding: 20px;
            margin-bottom: 20px;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        
        .stat-card {
            background-color: #fff;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
            padding: 20px;
            text-align: center;
        }
        
        .stat-value {
            font-size: 24px;
            font-weight: 600;
            color: var(--primary-color);
            margin: 10px 0;
        }
        
        .stat-label {
            font-size: 14px;
            color: #6c757d;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }
        
        th, td {
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
        }
        
        th {
            background-color: var(--secondary-color);
            font-weight: 600;
        }
        
        tr:hover {
            background-color: var(--hover-color);
        }
        
        .progress-container {
            width: 100%;
            background-color: #e9ecef;
            border-radius: 4px;
            margin: 10px 0;
        }
        
        .progress-bar {
            height: 10px;
            border-radius: 4px;
            background-color: var(--primary-color);
        }
        
        .actions {
            margin-top: 20px;
            display: flex;
            gap: 10px;
        }
        
        .btn {
            padding: 8px 16px;
            border-radius: 4px;
            font-size: 14px;
            font-weight: 500;
            cursor: pointer;
            border: none;
            transition: all 0.2s;
        }
        
        .btn-primary {
            background-color: var(--primary-color);
            color: white;
        }
        
        .btn-danger {
            background-color: #dc3545;
            color: white;
        }
        
        .btn:hover {
            opacity: 0.9;
        }
        
        .refresh-time {
            font-size: 12px;
            color: #6c757d;
            text-align: right;
            margin-top: 10px;
        }
        
        .back-button {
            display: inline-block;
            padding: 10px 20px;
            background-color: var(--primary-color);
            color: white;
            text-decoration: none;
            border-radius: 8px;
            font-size: 14px;
            transition: background-color 0.3s ease;
        }

        .back-button:hover {
            opacity: 0.9;
        }

        th a {
            color: var(--text-color);
            text-decoration: none;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        th a:hover {
            color: var(--primary-color);
        }
    </style>
</head>
<body>
    <div class="container">
        
        <a href="index.php" class="back-button">← Back to Dashboard</a>
        
        <h1 class="header" >Redis Cache Monitor - Tuesday Dashboard</h1>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Total Tuesday Keys</div>
                <div class="stat-value"><?php echo $totalTuesdayKeys; ?></div>
                <div class="stat-label">of <?php echo $dbSize; ?> total keys</div>
            </div>
            
            <div class="stat-card">
                <div class="stat-label">Memory Usage</div>
                <div class="stat-value"><?php echo $usedMemory; ?></div>
                <div class="stat-label">Peak: <?php echo $usedMemoryPeak; ?></div>
            </div>
            
            <div class="stat-card">
                <div class="stat-label">Cache Hit Ratio</div>
                <div class="stat-value"><?php echo $hitRatio; ?>%</div>
                <div class="stat-label">Hits: <?php echo number_format($keyspaceHits); ?> / Misses: <?php echo number_format($keyspaceMisses); ?></div>
                <div class="progress-container">
                    <div class="progress-bar" style="width: <?php echo $hitRatio; ?>%"></div>
                </div>
            </div>
            
            <div class="stat-card">
                <div class="stat-label">Redis Version</div>
                <div class="stat-value"><?php echo $info['redis_version']; ?></div>
                <div class="stat-label">Uptime: <?php echo round($info['uptime_in_seconds'] / 86400, 1); ?> days</div>
            </div>
        </div>
        
        <div class="card">
            <h2 class="card-header" >Tuesday Cache Keys</h2>
            <table>
                <thead>
                    <tr>
                        <th><a href="?sort=key&order=<?php echo $sortField === 'key' && $sortOrder === 'asc' ? 'desc' : 'asc'; ?>">Key <?php echo $sortField === 'key' ? ($sortOrder === 'asc' ? '▲' : '▼') : ''; ?></a></th>
                        <th><a href="?sort=type&order=<?php echo $sortField === 'type' && $sortOrder === 'asc' ? 'desc' : 'asc'; ?>">Type <?php echo $sortField === 'type' ? ($sortOrder === 'asc' ? '▲' : '▼') : ''; ?></a></th>
                        <th><a href="?sort=size&order=<?php echo $sortField === 'size' && $sortOrder === 'asc' ? 'desc' : 'asc'; ?>">Size <?php echo $sortField === 'size' ? ($sortOrder === 'asc' ? '▲' : '▼') : ''; ?></a></th>
                        <th><a href="?sort=expires_in&order=<?php echo $sortField === 'expires_in' && $sortOrder === 'asc' ? 'desc' : 'asc'; ?>">Expires In <?php echo $sortField === 'expires_in' ? ($sortOrder === 'asc' ? '▲' : '▼') : ''; ?></a></th>
                        <th><a href="?sort=created_at&order=<?php echo $sortField === 'created_at' && $sortOrder === 'asc' ? 'desc' : 'asc'; ?>">Created At <?php echo $sortField === 'created_at' ? ($sortOrder === 'asc' ? '▲' : '▼') : ''; ?></a></th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($keyDetails as $key): ?>
                    <tr>
                        <td><?php echo htmlspecialchars($key['key']); ?></td>
                        <td><?php echo $key['type']; ?></td>
                        <td><?php echo $key['size']; ?></td>
                        <td><?php echo $key['expires_in']; ?></td>
                        <td><?php echo $key['created_at']; ?></td>
                        <td>
                            <form method="post" style="display: inline;">
                                <input type="hidden" name="delete_key" value="<?php echo htmlspecialchars($key['key']); ?>">
                                <button type="submit" class="btn btn-danger">Delete</button>
                            </form>
                        </td>
                    </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
        
        <div class="actions">
            <form method="post">
                <input type="hidden" name="flush_tuesday" value="1">
                <button type="submit" class="btn btn-danger">Flush Tuesday Cache</button>
            </form>
            
            <form method="post">
                <button type="submit" class="btn btn-primary">Refresh</button>
            </form>
        </div>
        
        <div class="refresh-time">
            Last refreshed: <?php echo date('Y-m-d H:i:s'); ?>
        </div>
    </div>
</body>
</html>
