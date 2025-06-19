<?php
// Database connection details
$host = 'localhost';
$dbname = 'xx';  // Database name updated
$username = 'xx';
$password = 'xxx';

// Redis configuration
$redisHost = 'localhost';
$redisPort = 6379;
$redisPassword = null; // Set this if your Redis server requires authentication
$redisTimeout = 2.5;
$redisExpiry = 2592000; // Cache expiry in seconds (30 days)

// Initialize Redis connection
$redis = null;
try {
    $redis = new Redis();
    $connected = $redis->connect($redisHost, $redisPort, $redisTimeout);
    if ($connected && $redisPassword) {
        $redis->auth($redisPassword);
    }
    if (!$connected) {
        error_log("Redis connection failed");
        $redis = null;
    }
} catch (Exception $e) {
    error_log("Redis error: " . $e->getMessage());
    $redis = null;
}

// Create connection
$conn = new mysqli($host, $username, $password, $dbname);

// Check for connection error
if ($conn->connect_error) {
    die(json_encode(array("error" => "Connection failed: " . $conn->connect_error), JSON_UNESCAPED_UNICODE));
}

// Set the charset to UTF-8 (important for handling special characters)
$conn->set_charset("utf8mb4"); 

// Get action parameter (export or normal request)
$action = isset($_GET['action']) ? $_GET['action'] : (isset($_POST['action']) ? $_POST['action'] : '');

// Function to sanitize the filename (removes unwanted characters)
function sanitizeFileName($string) {
    return preg_replace('/[^a-zA-Z0-9-_\.]/', '_', $string);
}

// Function to apply filter based on operator
function applyFilter($field, $value, $operator = 'contains') {
    global $conn;
    $value = $conn->real_escape_string($value);
    
    switch ($operator) {
        case 'starts_with':
            return "`$field` LIKE '$value%'";
        case 'include':
            // Include matches exact sequence
            return "`$field` LIKE '%$value%'";
        case 'exclude':
            // Exclude matches if sequence does not appear
            return "`$field` NOT LIKE '%$value%'";
        case 'contains':
        default:
            return "`$field` LIKE '%$value%'";
    }
}

// Function to generate a cache key based on request parameters
function generateCacheKey($params) {
    // Sort parameters to ensure consistent cache keys
    ksort($params);
    return 'tuesday_' . md5(json_encode($params));
}

// Get unique countries
if ($action === 'getCountries') {
    $cacheKey = 'tuesday_countries';
    $countries = [];
    
    // Try to get from cache first
    if ($redis && $redis->exists($cacheKey)) {
        $countries = json_decode($redis->get($cacheKey), true);
        echo json_encode(['success' => true, 'countries' => $countries, 'cached' => true]);
        exit;
    }
    
    $sql = "SELECT DISTINCT country FROM data WHERE country IS NOT NULL AND country != '' ORDER BY country";
    $result = $conn->query($sql);
    
    if ($result) {
        while ($row = $result->fetch_assoc()) {
            $countries[] = $row['country'];
        }
        
        // Store in cache
        if ($redis) {
            $redis->set($cacheKey, json_encode($countries));
            $redis->expire($cacheKey, $redisExpiry);
        }
        
        echo json_encode(['success' => true, 'countries' => $countries, 'cached' => false]);
    } else {
        echo json_encode(['success' => false, 'error' => $conn->error]);
    }
    exit;
}

// Get unique cities
if ($action === 'getCities') {
    $cacheKey = 'tuesday_cities';
    $cities = [];
    
    // Try to get from cache first
    if ($redis && $redis->exists($cacheKey)) {
        $cities = json_decode($redis->get($cacheKey), true);
        echo json_encode(['success' => true, 'cities' => $cities, 'cached' => true]);
        exit;
    }
    
    $sql = "SELECT DISTINCT city FROM data WHERE city IS NOT NULL AND city != '' ORDER BY city";
    $result = $conn->query($sql);
    
    if ($result) {
        while ($row = $result->fetch_assoc()) {
            $cities[] = $row['city'];
        }
        
        // Store in cache
        if ($redis) {
            $redis->set($cacheKey, json_encode($cities));
            $redis->expire($cacheKey, $redisExpiry);
        }
        
        echo json_encode(['success' => true, 'cities' => $cities, 'cached' => false]);
    } else {
        echo json_encode(['success' => false, 'error' => $conn->error]);
    }
    exit;
}

// Check if CSV export is requested
if ($action === 'exportToCSV') {
    // Get filter values from request
    $searchValue = isset($_GET['searchValue']) ? $_GET['searchValue'] : "";
    
    // Org Name filters
    $orgNameContains = isset($_GET['orgNameContains']) ? $_GET['orgNameContains'] : "";
    $orgNameStartsWith = isset($_GET['orgNameStartsWith']) ? $_GET['orgNameStartsWith'] : "";
    $orgNameIncludes = isset($_GET['orgNameIncludes']) ? $_GET['orgNameIncludes'] : "";
    $orgNameExcludes = isset($_GET['orgNameExcludes']) ? $_GET['orgNameExcludes'] : "";
    
    // Org Website filters
    $orgWebsiteContains = isset($_GET['orgWebsiteContains']) ? $_GET['orgWebsiteContains'] : "";
    $orgWebsiteStartsWith = isset($_GET['orgWebsiteStartsWith']) ? $_GET['orgWebsiteStartsWith'] : "";
    $orgWebsiteIncludes = isset($_GET['orgWebsiteIncludes']) ? $_GET['orgWebsiteIncludes'] : "";
    $orgWebsiteExcludes = isset($_GET['orgWebsiteExcludes']) ? $_GET['orgWebsiteExcludes'] : "";
    
    // Seniority filters
    $seniorityContains = isset($_GET['seniorityContains']) ? $_GET['seniorityContains'] : "";
    $seniorityStartsWith = isset($_GET['seniorityStartsWith']) ? $_GET['seniorityStartsWith'] : "";
    $seniorityIncludes = isset($_GET['seniorityIncludes']) ? $_GET['seniorityIncludes'] : "";
    $seniorityExcludes = isset($_GET['seniorityExcludes']) ? $_GET['seniorityExcludes'] : "";
    
    // Title filters
    $titleContains = isset($_GET['titleContains']) ? $_GET['titleContains'] : "";
    $titleStartsWith = isset($_GET['titleStartsWith']) ? $_GET['titleStartsWith'] : "";
    $titleIncludes = isset($_GET['titleIncludes']) ? $_GET['titleIncludes'] : "";
    $titleExcludes = isset($_GET['titleExcludes']) ? $_GET['titleExcludes'] : "";
    
    // Org Founded Year filter
    $orgFoundedYear = isset($_GET['orgFoundedYear']) ? $_GET['orgFoundedYear'] : "";
    
    // Country filter
    $country = isset($_GET['country']) ? json_decode($_GET['country'], true) : [];
    
    // City filter
    $city = isset($_GET['city']) ? json_decode($_GET['city'], true) : [];

    // Apply limit if exportLimit is set
    $exportLimit = isset($_GET['limit']) && $_GET['limit'] !== '' ? (int)$_GET['limit'] : null;

    // Prepare selected columns
    $selectedColumns = isset($_GET['columns']) ? $_GET['columns'] : '';

    // Escape the filter values to prevent SQL injection
    $searchValue = $conn->real_escape_string($searchValue);
    
    $orgNameContains = $conn->real_escape_string($orgNameContains);
    $orgNameStartsWith = $conn->real_escape_string($orgNameStartsWith);
    $orgNameIncludes = $conn->real_escape_string($orgNameIncludes);
    $orgNameExcludes = $conn->real_escape_string($orgNameExcludes);
    
    $orgWebsiteContains = $conn->real_escape_string($orgWebsiteContains);
    $orgWebsiteStartsWith = $conn->real_escape_string($orgWebsiteStartsWith);
    $orgWebsiteIncludes = $conn->real_escape_string($orgWebsiteIncludes);
    $orgWebsiteExcludes = $conn->real_escape_string($orgWebsiteExcludes);
    
    $seniorityContains = $conn->real_escape_string($seniorityContains);
    $seniorityStartsWith = $conn->real_escape_string($seniorityStartsWith);
    $seniorityIncludes = $conn->real_escape_string($seniorityIncludes);
    $seniorityExcludes = $conn->real_escape_string($seniorityExcludes);
    
    $titleContains = $conn->real_escape_string($titleContains);
    $titleStartsWith = $conn->real_escape_string($titleStartsWith);
    $titleIncludes = $conn->real_escape_string($titleIncludes);
    $titleExcludes = $conn->real_escape_string($titleExcludes);
    
    $orgFoundedYear = $conn->real_escape_string($orgFoundedYear);

    // Default columns if none selected
    if (!empty($selectedColumns)) {
        $columnsArray = explode(',', $selectedColumns);
        $columnsToSelect = array_map(function($col) {
            return "`" . trim($col) . "`"; // Wrap each column name in backticks
        }, $columnsArray);
        $columnsToSelect = implode(', ', $columnsToSelect);
    } else {
        // Default columns to export all
        $columnsToSelect = '*';
    }
 
    // Get start and end rows
    $startRow = isset($_GET['startRow']) ? (int)$_GET['startRow'] : 0;
    $endRow = isset($_GET['endRow']) ? (int)$_GET['endRow'] : 0;

    // Generate cache key for export
    $exportParams = array_merge($_GET, ['action' => 'exportToCSV']);
    $cacheKey = 'tuesday_export_' . md5(json_encode($exportParams));
    
    // For exports, we'll only cache the SQL query, not the actual CSV data
    $sql = null;
    if ($redis && $redis->exists($cacheKey)) {
        $sql = $redis->get($cacheKey);
    } else {
        // Construct the SQL query for fetching the data with proper filter conditions
        $sql = "SELECT $columnsToSelect FROM data WHERE 1=1";

        // Apply global search filter
        if (!empty($searchValue)) {
            // Multiple field search
            $sql .= " AND (";
            $sql .= applyFilter('account_name', $searchValue, 'contains');
            $sql .= " OR " . applyFilter('account_website', $searchValue, 'contains');
            $sql .= " OR " . applyFilter('account_industry', $searchValue, 'contains');
            $sql .= ")";
        }
        
        // Apply Org Name filters
        if (!empty($orgNameContains)) {
            $sql .= " AND " . applyFilter('org_name', $orgNameContains, 'contains');
        }
        if (!empty($orgNameStartsWith)) {
            $sql .= " AND " . applyFilter('org_name', $orgNameStartsWith, 'starts_with');
        }
        if (!empty($orgNameIncludes)) {
            $sql .= " AND " . applyFilter('org_name', $orgNameIncludes, 'include');
        }
        if (!empty($orgNameExcludes)) {
            $sql .= " AND " . applyFilter('org_name', $orgNameExcludes, 'exclude');
        }
        
        // Apply Org Website filters
        if (!empty($orgWebsiteContains)) {
            $sql .= " AND " . applyFilter('org_website_url', $orgWebsiteContains, 'contains');
        }
        if (!empty($orgWebsiteStartsWith)) {
            $sql .= " AND " . applyFilter('org_website_url', $orgWebsiteStartsWith, 'starts_with');
        }
        if (!empty($orgWebsiteIncludes)) {
            $sql .= " AND " . applyFilter('org_website_url', $orgWebsiteIncludes, 'include');
        }
        if (!empty($orgWebsiteExcludes)) {
            $sql .= " AND " . applyFilter('org_website_url', $orgWebsiteExcludes, 'exclude');
        }
        
        // Apply Seniority filters
        if (!empty($seniorityContains)) {
            $sql .= " AND " . applyFilter('seniority', $seniorityContains, 'contains');
        }
        if (!empty($seniorityStartsWith)) {
            $sql .= " AND " . applyFilter('seniority', $seniorityStartsWith, 'starts_with');
        }
        if (!empty($seniorityIncludes)) {
            $sql .= " AND " . applyFilter('seniority', $seniorityIncludes, 'include');
        }
        if (!empty($seniorityExcludes)) {
            $sql .= " AND " . applyFilter('seniority', $seniorityExcludes, 'exclude');
        }
        
        // Apply Title filters
        if (!empty($titleContains)) {
            $sql .= " AND " . applyFilter('title', $titleContains, 'contains');
        }
        if (!empty($titleStartsWith)) {
            $sql .= " AND " . applyFilter('title', $titleStartsWith, 'starts_with');
        }
        if (!empty($titleIncludes)) {
            $sql .= " AND " . applyFilter('title', $titleIncludes, 'include');
        }
        if (!empty($titleExcludes)) {
            $sql .= " AND " . applyFilter('title', $titleExcludes, 'exclude');
        }
        
        // Apply Org Founded Year filter
        if (!empty($orgFoundedYear)) {
            $sql .= " AND `org_founded_year` = '$orgFoundedYear'";
        }
        
        // Apply Country filter
        if (!empty($country)) {
            $countryConditions = [];
            foreach ($country as $c) {
                $escapedCountry = $conn->real_escape_string($c);
                $countryConditions[] = "`country` = '$escapedCountry'";
            }
            if (!empty($countryConditions)) {
                $sql .= " AND (" . implode(" OR ", $countryConditions) . ")";
            }
        }
        
        // Apply City filter
        if (!empty($city)) {
            $cityConditions = [];
            foreach ($city as $c) {
                $escapedCity = $conn->real_escape_string($c);
                $cityConditions[] = "`city` = '$escapedCity'";
            }
            if (!empty($cityConditions)) {
                $sql .= " AND (" . implode(" OR ", $cityConditions) . ")";
            }
        }
        
        // Apply row offset if startRow and endRow are set
        if ($startRow > 0 && $endRow > 0) {
            $sql .= " LIMIT " . ($startRow - 1) . ", " . ($endRow - $startRow + 1);
        }
        // Apply export limit if provided
        if ($exportLimit !== null) {
            $sql .= " LIMIT $exportLimit"; // Apply the export limit if provided
        }
        
        // Cache the SQL query for future exports with the same parameters
        if ($redis) {
            $redis->set($cacheKey, $sql);
            $redis->expire($cacheKey, $redisExpiry);
        }
    }

    // Execute the query
    $result = $conn->query($sql);

    // Check if the query was successful
    if ($result === false) {
        die('Query failed: ' . $conn->error);
    }

    // Generate dynamic file name based on filters
    $fileName = 'Tuesday_Export_';
    $filtersApplied = false; // Track if any filter is applied

    // Check if each filter is applied, and append to the file name if so
    if (!empty($searchValue)) {
        $fileName .= 'Search_';
        $filtersApplied = true;
    }

    if (!empty($orgNameContains) || !empty($orgNameStartsWith) || !empty($orgNameIncludes) || !empty($orgNameExcludes)) {
        $fileName .= 'OrgName_';
        $filtersApplied = true;
    }
    if (!empty($orgWebsiteContains) || !empty($orgWebsiteStartsWith) || !empty($orgWebsiteIncludes) || !empty($orgWebsiteExcludes)) {
        $fileName .= 'OrgWebsite_';
        $filtersApplied = true;
    }
    if (!empty($seniorityContains) || !empty($seniorityStartsWith) || !empty($seniorityIncludes) || !empty($seniorityExcludes)) {
        $fileName .= 'Seniority_';
        $filtersApplied = true;
    }
    if (!empty($titleContains) || !empty($titleStartsWith) || !empty($titleIncludes) || !empty($titleExcludes)) {
        $fileName .= 'Title_';
        $filtersApplied = true;
    }
    if (!empty($orgFoundedYear)) {
        $fileName .= 'OrgFoundedYear_';
        $filtersApplied = true;
    }
    if (!empty($country)) {
        $fileName .= 'Country_';
        $filtersApplied = true;
    }
    if (!empty($city)) {
        $fileName .= 'City_';
        $filtersApplied = true;
    }

    // If no filters were applied, use a default name
    if (!$filtersApplied) {
        $fileName .= 'All_Records_';
    }

    // Add date for uniqueness
    $fileName .= date('Y-m-d_H-i-s') . '.csv';

    // Ensure the filename doesn't contain any unwanted characters
    $fileName = sanitizeFileName($fileName);

    // Open the output stream for CSV
    header('Content-Type: text/csv');
    header('Content-Disposition: attachment; filename="' . $fileName . '"');
    $output = fopen('php://output', 'w');

    // Fetch column names from the result and write them as header
    $columns = $result->fetch_fields();
    $header = [];
    foreach ($columns as $column) {
        $header[] = $column->name;
    }
    fputcsv($output, $header);

    // Fetch and write the data rows
    while ($row = $result->fetch_assoc()) {
        fputcsv($output, $row);
    }

    // Close the output stream and database connection
    fclose($output);
    $conn->close();
    exit;  // Terminate script to prevent further output
}

// Get the filter values from the request
$searchValue = isset($_POST['searchValue']) ? $_POST['searchValue'] : '';

// Org Name filters
$orgNameContains = isset($_POST['orgNameContains']) ? $_POST['orgNameContains'] : '';
$orgNameStartsWith = isset($_POST['orgNameStartsWith']) ? $_POST['orgNameStartsWith'] : '';
$orgNameIncludes = isset($_POST['orgNameIncludes']) ? $_POST['orgNameIncludes'] : '';
$orgNameExcludes = isset($_POST['orgNameExcludes']) ? $_POST['orgNameExcludes'] : '';

// Org Website filters
$orgWebsiteContains = isset($_POST['orgWebsiteContains']) ? $_POST['orgWebsiteContains'] : '';
$orgWebsiteStartsWith = isset($_POST['orgWebsiteStartsWith']) ? $_POST['orgWebsiteStartsWith'] : '';
$orgWebsiteIncludes = isset($_POST['orgWebsiteIncludes']) ? $_POST['orgWebsiteIncludes'] : '';
$orgWebsiteExcludes = isset($_POST['orgWebsiteExcludes']) ? $_POST['orgWebsiteExcludes'] : '';

// Seniority filters
$seniorityContains = isset($_POST['seniorityContains']) ? $_POST['seniorityContains'] : '';
$seniorityStartsWith = isset($_POST['seniorityStartsWith']) ? $_POST['seniorityStartsWith'] : '';
$seniorityIncludes = isset($_POST['seniorityIncludes']) ? $_POST['seniorityIncludes'] : '';
$seniorityExcludes = isset($_POST['seniorityExcludes']) ? $_POST['seniorityExcludes'] : '';

// Title filters
$titleContains = isset($_POST['titleContains']) ? $_POST['titleContains'] : '';
$titleStartsWith = isset($_POST['titleStartsWith']) ? $_POST['titleStartsWith'] : '';
$titleIncludes = isset($_POST['titleIncludes']) ? $_POST['titleIncludes'] : '';
$titleExcludes = isset($_POST['titleExcludes']) ? $_POST['titleExcludes'] : '';

// Org Founded Year filter
$orgFoundedYear = isset($_POST['orgFoundedYear']) ? $_POST['orgFoundedYear'] : '';

// Country filter
$country = isset($_POST['country']) ? $_POST['country'] : [];

// City filter
$city = isset($_POST['city']) ? $_POST['city'] : [];

// Get the page number and limit from the AJAX request (default to 1 and 10 if not provided)
$page = isset($_POST['page']) ? (int)$_POST['page'] : 1;
$limit = isset($_POST['limit']) ? (int)$_POST['limit'] : 10;

// Calculate the offset for the query
$offset = ($page - 1) * $limit;

// Generate cache key for this query
$cacheKey = generateCacheKey(array_merge($_POST, ['query' => 'main_data']));

// Try to get data from cache first
$cachedData = null;
if ($redis && $redis->exists($cacheKey)) {
    $cachedData = json_decode($redis->get($cacheKey), true);
}

if ($cachedData !== null) {
    // Use cached data
    header('Content-Type: application/json');
    echo json_encode($cachedData);
    exit;
}

// Escape all inputs
$searchValue = $conn->real_escape_string($searchValue);

$orgNameContains = $conn->real_escape_string($orgNameContains);
$orgNameStartsWith = $conn->real_escape_string($orgNameStartsWith);
$orgNameIncludes = $conn->real_escape_string($orgNameIncludes);
$orgNameExcludes = $conn->real_escape_string($orgNameExcludes);

$orgWebsiteContains = $conn->real_escape_string($orgWebsiteContains);
$orgWebsiteStartsWith = $conn->real_escape_string($orgWebsiteStartsWith);
$orgWebsiteIncludes = $conn->real_escape_string($orgWebsiteIncludes);
$orgWebsiteExcludes = $conn->real_escape_string($orgWebsiteExcludes);

$seniorityContains = $conn->real_escape_string($seniorityContains);
$seniorityStartsWith = $conn->real_escape_string($seniorityStartsWith);
$seniorityIncludes = $conn->real_escape_string($seniorityIncludes);
$seniorityExcludes = $conn->real_escape_string($seniorityExcludes);

$titleContains = $conn->real_escape_string($titleContains);
$titleStartsWith = $conn->real_escape_string($titleStartsWith);
$titleIncludes = $conn->real_escape_string($titleIncludes);
$titleExcludes = $conn->real_escape_string($titleExcludes);

$orgFoundedYear = $conn->real_escape_string($orgFoundedYear);

// Construct main SQL query
$sql = "SELECT * FROM data WHERE 1=1";

// Apply global search filter
if (!empty($searchValue)) {
    // Search across multiple fields
    $sql .= " AND (";
    $sql .= applyFilter('account_name', $searchValue, 'contains');
    $sql .= " OR " . applyFilter('account_website', $searchValue, 'contains');
    $sql .= " OR " . applyFilter('account_industry', $searchValue, 'contains');
    $sql .= ")";
}

// Apply Org Name filters
if (!empty($orgNameContains)) {
    $sql .= " AND " . applyFilter('org_name', $orgNameContains, 'contains');
}
if (!empty($orgNameStartsWith)) {
    $sql .= " AND " . applyFilter('org_name', $orgNameStartsWith, 'starts_with');
}
if (!empty($orgNameIncludes)) {
    $sql .= " AND " . applyFilter('org_name', $orgNameIncludes, 'include');
}
if (!empty($orgNameExcludes)) {
    $sql .= " AND " . applyFilter('org_name', $orgNameExcludes, 'exclude');
}

// Apply Org Website filters
if (!empty($orgWebsiteContains)) {
    $sql .= " AND " . applyFilter('org_website_url', $orgWebsiteContains, 'contains');
}
if (!empty($orgWebsiteStartsWith)) {
    $sql .= " AND " . applyFilter('org_website_url', $orgWebsiteStartsWith, 'starts_with');
}
if (!empty($orgWebsiteIncludes)) {
    $sql .= " AND " . applyFilter('org_website_url', $orgWebsiteIncludes, 'include');
}
if (!empty($orgWebsiteExcludes)) {
    $sql .= " AND " . applyFilter('org_website_url', $orgWebsiteExcludes, 'exclude');
}

// Apply Seniority filters
if (!empty($seniorityContains)) {
    $sql .= " AND " . applyFilter('seniority', $seniorityContains, 'contains');
}
if (!empty($seniorityStartsWith)) {
    $sql .= " AND " . applyFilter('seniority', $seniorityStartsWith, 'starts_with');
}
if (!empty($seniorityIncludes)) {
    $sql .= " AND " . applyFilter('seniority', $seniorityIncludes, 'include');
}
if (!empty($seniorityExcludes)) {
    $sql .= " AND " . applyFilter('seniority', $seniorityExcludes, 'exclude');
}

// Apply Title filters
if (!empty($titleContains)) {
    $sql .= " AND " . applyFilter('title', $titleContains, 'contains');
}
if (!empty($titleStartsWith)) {
    $sql .= " AND " . applyFilter('title', $titleStartsWith, 'starts_with');
}
if (!empty($titleIncludes)) {
    $sql .= " AND " . applyFilter('title', $titleIncludes, 'include');
}
if (!empty($titleExcludes)) {
    $sql .= " AND " . applyFilter('title', $titleExcludes, 'exclude');
}

// Apply Org Founded Year filter
if (!empty($orgFoundedYear)) {
    $sql .= " AND `org_founded_year` = '$orgFoundedYear'";
}

// Apply Country filter
if (!empty($country)) {
    $countryConditions = [];
    foreach ($country as $c) {
        $escapedCountry = $conn->real_escape_string($c);
        $countryConditions[] = "`country` = '$escapedCountry'";
    }
    if (!empty($countryConditions)) {
        $sql .= " AND (" . implode(" OR ", $countryConditions) . ")";
    }
}

// Apply City filter
if (!empty($city)) {
    $cityConditions = [];
    foreach ($city as $c) {
        $escapedCity = $conn->real_escape_string($c);
        $cityConditions[] = "`city` = '$escapedCity'";
    }
    if (!empty($cityConditions)) {
        $sql .= " AND (" . implode(" OR ", $cityConditions) . ")";
    }
}

// Count total records with filters before adding LIMIT
$totalFilteredRecordsSql = str_replace("SELECT *", "SELECT COUNT(*) as total", $sql);

// Try to get filtered count from cache
$filteredCountCacheKey = 'tuesday_filtered_count_' . md5($totalFilteredRecordsSql);
$totalFilteredRecords = 0;

if ($redis && $redis->exists($filteredCountCacheKey)) {
    $totalFilteredRecords = (int)$redis->get($filteredCountCacheKey);
} else {
    $totalFilteredRecordsResult = $conn->query($totalFilteredRecordsSql);
    if (!$totalFilteredRecordsResult) {
        die("Error executing filtered records query: " . $conn->error);
    }
    $totalFilteredRecords = $totalFilteredRecordsResult->fetch_assoc()['total'];
    if ($totalFilteredRecords === null) {
        $totalFilteredRecords = 0;
    }
    
    // Cache the filtered count
    if ($redis) {
        $redis->set($filteredCountCacheKey, $totalFilteredRecords);
        $redis->expire($filteredCountCacheKey, $redisExpiry);
    }
}

// Add LIMIT and OFFSET to the main query
$sql .= " LIMIT $limit OFFSET $offset";

// Execute main query
$result = $conn->query($sql);

if ($result === false) {
    die(json_encode(array("error" => "Query failed: " . $conn->error)));
}

$data = array();
if ($result->num_rows > 0) {
    while ($row = $result->fetch_assoc()) {
        $data[] = $row;
    }
}

// Get total records in the table (without any filter)
$totalRecordsCacheKey = 'tuesday_total_records';
$totalRecords = 0;

// Try to get total records from cache
if ($redis && $redis->exists($totalRecordsCacheKey)) {
    $totalRecords = (int)$redis->get($totalRecordsCacheKey);
} else {
    $totalRecordsSql = "SELECT COUNT(*) as total FROM data";
    $totalRecordsResult = $conn->query($totalRecordsSql);
    if (!$totalRecordsResult) {
        die("Error executing total records query: " . $conn->error);
    }
    $totalRecords = $totalRecordsResult->fetch_assoc()['total'];
    
    // Cache the total records count
    if ($redis) {
        $redis->set($totalRecordsCacheKey, $totalRecords);
        $redis->expire($totalRecordsCacheKey, $redisExpiry * 24); // Cache for longer since this rarely changes
    }
}

// Calculate total pages
$totalPages = ceil($totalFilteredRecords / $limit);

// Prepare response
$response = [
    "draw" => isset($_POST['draw']) ? (int)$_POST['draw'] : 1,
    "recordsTotal" => $totalRecords,
    "recordsFiltered" => $totalFilteredRecords,
    "data" => $data,
    "totalPages" => $totalPages,
    "start" => $offset,
    "totalEntries" => $totalRecords,
    "cached" => false
];

// Cache the response
if ($redis) {
    $redis->set($cacheKey, json_encode($response));
    $redis->expire($cacheKey, $redisExpiry);
}

// Close the connection
$conn->close();

// Set JSON header and return the response
header('Content-Type: application/json');
echo json_encode($response, JSON_UNESCAPED_UNICODE);
?>
