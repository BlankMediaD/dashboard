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
    
    $sql = "SELECT DISTINCT account_country FROM data WHERE account_country IS NOT NULL AND account_country != '' ORDER BY account_country";
    $result = $conn->query($sql);
    
    if ($result) {
        while ($row = $result->fetch_assoc()) {
            $countries[] = $row['account_country'];
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
    
    $sql = "SELECT DISTINCT account_city FROM data WHERE account_city IS NOT NULL AND account_city != '' ORDER BY account_city";
    $result = $conn->query($sql);
    
    if ($result) {
        while ($row = $result->fetch_assoc()) {
            $cities[] = $row['account_city'];
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
    
    // Account Name filters
    $accountNameContains = isset($_GET['accountNameContains']) ? $_GET['accountNameContains'] : "";
    $accountNameStartsWith = isset($_GET['accountNameStartsWith']) ? $_GET['accountNameStartsWith'] : "";
    $accountNameIncludes = isset($_GET['accountNameIncludes']) ? $_GET['accountNameIncludes'] : "";
    $accountNameExcludes = isset($_GET['accountNameExcludes']) ? $_GET['accountNameExcludes'] : "";
    
    // Website filters
    $accountWebsiteContains = isset($_GET['accountWebsiteContains']) ? $_GET['accountWebsiteContains'] : "";
    $accountWebsiteStartsWith = isset($_GET['accountWebsiteStartsWith']) ? $_GET['accountWebsiteStartsWith'] : "";
    $accountWebsiteIncludes = isset($_GET['accountWebsiteIncludes']) ? $_GET['accountWebsiteIncludes'] : "";
    $accountWebsiteExcludes = isset($_GET['accountWebsiteExcludes']) ? $_GET['accountWebsiteExcludes'] : "";
    
    // Industry filters
    $accountIndustryContains = isset($_GET['accountIndustryContains']) ? $_GET['accountIndustryContains'] : "";
    $accountIndustryStartsWith = isset($_GET['accountIndustryStartsWith']) ? $_GET['accountIndustryStartsWith'] : "";
    $accountIndustryIncludes = isset($_GET['accountIndustryIncludes']) ? $_GET['accountIndustryIncludes'] : "";
    $accountIndustryExcludes = isset($_GET['accountIndustryExcludes']) ? $_GET['accountIndustryExcludes'] : "";
    
    // Employee Count filters
    $accountEmployeeCountContains = isset($_GET['accountEmployeeCountContains']) ? $_GET['accountEmployeeCountContains'] : "";
    $accountEmployeeCountStartsWith = isset($_GET['accountEmployeeCountStartsWith']) ? $_GET['accountEmployeeCountStartsWith'] : "";
    $accountEmployeeCountIncludes = isset($_GET['accountEmployeeCountIncludes']) ? $_GET['accountEmployeeCountIncludes'] : "";
    $accountEmployeeCountExcludes = isset($_GET['accountEmployeeCountExcludes']) ? $_GET['accountEmployeeCountExcludes'] : "";
    
    // Founded Year filter
    $accountFoundedYear = isset($_GET['accountFoundedYear']) ? $_GET['accountFoundedYear'] : "";
    
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
    
    $accountNameContains = $conn->real_escape_string($accountNameContains);
    $accountNameStartsWith = $conn->real_escape_string($accountNameStartsWith);
    $accountNameIncludes = $conn->real_escape_string($accountNameIncludes);
    $accountNameExcludes = $conn->real_escape_string($accountNameExcludes);
    
    $accountWebsiteContains = $conn->real_escape_string($accountWebsiteContains);
    $accountWebsiteStartsWith = $conn->real_escape_string($accountWebsiteStartsWith);
    $accountWebsiteIncludes = $conn->real_escape_string($accountWebsiteIncludes);
    $accountWebsiteExcludes = $conn->real_escape_string($accountWebsiteExcludes);
    
    $accountIndustryContains = $conn->real_escape_string($accountIndustryContains);
    $accountIndustryStartsWith = $conn->real_escape_string($accountIndustryStartsWith);
    $accountIndustryIncludes = $conn->real_escape_string($accountIndustryIncludes);
    $accountIndustryExcludes = $conn->real_escape_string($accountIndustryExcludes);
    
    $accountEmployeeCountContains = $conn->real_escape_string($accountEmployeeCountContains);
    $accountEmployeeCountStartsWith = $conn->real_escape_string($accountEmployeeCountStartsWith);
    $accountEmployeeCountIncludes = $conn->real_escape_string($accountEmployeeCountIncludes);
    $accountEmployeeCountExcludes = $conn->real_escape_string($accountEmployeeCountExcludes);
    
    $accountFoundedYear = $conn->real_escape_string($accountFoundedYear);

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
        
        // Apply Account Name filters
        if (!empty($accountNameContains)) {
            $sql .= " AND " . applyFilter('account_name', $accountNameContains, 'contains');
        }
        if (!empty($accountNameStartsWith)) {
            $sql .= " AND " . applyFilter('account_name', $accountNameStartsWith, 'starts_with');
        }
        if (!empty($accountNameIncludes)) {
            $sql .= " AND " . applyFilter('account_name', $accountNameIncludes, 'include');
        }
        if (!empty($accountNameExcludes)) {
            $sql .= " AND " . applyFilter('account_name', $accountNameExcludes, 'exclude');
        }
        
        // Apply Website filters
        if (!empty($accountWebsiteContains)) {
            $sql .= " AND " . applyFilter('account_website', $accountWebsiteContains, 'contains');
        }
        if (!empty($accountWebsiteStartsWith)) {
            $sql .= " AND " . applyFilter('account_website', $accountWebsiteStartsWith, 'starts_with');
        }
        if (!empty($accountWebsiteIncludes)) {
            $sql .= " AND " . applyFilter('account_website', $accountWebsiteIncludes, 'include');
        }
        if (!empty($accountWebsiteExcludes)) {
            $sql .= " AND " . applyFilter('account_website', $accountWebsiteExcludes, 'exclude');
        }
        
        // Apply Industry filters
        if (!empty($accountIndustryContains)) {
            $sql .= " AND " . applyFilter('account_industry', $accountIndustryContains, 'contains');
        }
        if (!empty($accountIndustryStartsWith)) {
            $sql .= " AND " . applyFilter('account_industry', $accountIndustryStartsWith, 'starts_with');
        }
        if (!empty($accountIndustryIncludes)) {
            $sql .= " AND " . applyFilter('account_industry', $accountIndustryIncludes, 'include');
        }
        if (!empty($accountIndustryExcludes)) {
            $sql .= " AND " . applyFilter('account_industry', $accountIndustryExcludes, 'exclude');
        }
        
        // Apply Employee Count filters
        if (!empty($accountEmployeeCountContains)) {
            $sql .= " AND " . applyFilter('account_employee_count_range', $accountEmployeeCountContains, 'contains');
        }
        if (!empty($accountEmployeeCountStartsWith)) {
            $sql .= " AND " . applyFilter('account_employee_count_range', $accountEmployeeCountStartsWith, 'starts_with');
        }
        if (!empty($accountEmployeeCountIncludes)) {
            $sql .= " AND " . applyFilter('account_employee_count_range', $accountEmployeeCountIncludes, 'include');
        }
        if (!empty($accountEmployeeCountExcludes)) {
            $sql .= " AND " . applyFilter('account_employee_count_range', $accountEmployeeCountExcludes, 'exclude');
        }
        
        // Apply Founded Year filter
        if (!empty($accountFoundedYear)) {
            $sql .= " AND `account_founded_year` = '$accountFoundedYear'";
        }
        
        // Apply Country filter
        if (!empty($country)) {
            $countryConditions = [];
            foreach ($country as $c) {
                $escapedCountry = $conn->real_escape_string($c);
                $countryConditions[] = "`account_country` = '$escapedCountry'";
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
                $cityConditions[] = "`account_city` = '$escapedCity'";
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

    if (!empty($accountNameContains) || !empty($accountNameStartsWith) || !empty($accountNameIncludes) || !empty($accountNameExcludes)) {
        $fileName .= 'AccountName_';
        $filtersApplied = true;
    }
    if (!empty($accountWebsiteContains) || !empty($accountWebsiteStartsWith) || !empty($accountWebsiteIncludes) || !empty($accountWebsiteExcludes)) {
        $fileName .= 'Website_';
        $filtersApplied = true;
    }
    if (!empty($accountIndustryContains) || !empty($accountIndustryStartsWith) || !empty($accountIndustryIncludes) || !empty($accountIndustryExcludes)) {
        $fileName .= 'Industry_';
        $filtersApplied = true;
    }
    if (!empty($accountEmployeeCountContains) || !empty($accountEmployeeCountStartsWith) || !empty($accountEmployeeCountIncludes) || !empty($accountEmployeeCountExcludes)) {
        $fileName .= 'EmployeeCount_';
        $filtersApplied = true;
    }
    if (!empty($accountFoundedYear)) {
        $fileName .= 'FoundedYear_';
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

// Account Name filters
$accountNameContains = isset($_POST['accountNameContains']) ? $_POST['accountNameContains'] : '';
$accountNameStartsWith = isset($_POST['accountNameStartsWith']) ? $_POST['accountNameStartsWith'] : '';
$accountNameIncludes = isset($_POST['accountNameIncludes']) ? $_POST['accountNameIncludes'] : '';
$accountNameExcludes = isset($_POST['accountNameExcludes']) ? $_POST['accountNameExcludes'] : '';

// Website filters
$accountWebsiteContains = isset($_POST['accountWebsiteContains']) ? $_POST['accountWebsiteContains'] : '';
$accountWebsiteStartsWith = isset($_POST['accountWebsiteStartsWith']) ? $_POST['accountWebsiteStartsWith'] : '';
$accountWebsiteIncludes = isset($_POST['accountWebsiteIncludes']) ? $_POST['accountWebsiteIncludes'] : '';
$accountWebsiteExcludes = isset($_POST['accountWebsiteExcludes']) ? $_POST['accountWebsiteExcludes'] : '';

// Industry filters
$accountIndustryContains = isset($_POST['accountIndustryContains']) ? $_POST['accountIndustryContains'] : '';
$accountIndustryStartsWith = isset($_POST['accountIndustryStartsWith']) ? $_POST['accountIndustryStartsWith'] : '';
$accountIndustryIncludes = isset($_POST['accountIndustryIncludes']) ? $_POST['accountIndustryIncludes'] : '';
$accountIndustryExcludes = isset($_POST['accountIndustryExcludes']) ? $_POST['accountIndustryExcludes'] : '';

// Employee Count filters
$accountEmployeeCountContains = isset($_POST['accountEmployeeCountContains']) ? $_POST['accountEmployeeCountContains'] : '';
$accountEmployeeCountStartsWith = isset($_POST['accountEmployeeCountStartsWith']) ? $_POST['accountEmployeeCountStartsWith'] : '';
$accountEmployeeCountIncludes = isset($_POST['accountEmployeeCountIncludes']) ? $_POST['accountEmployeeCountIncludes'] : '';
$accountEmployeeCountExcludes = isset($_POST['accountEmployeeCountExcludes']) ? $_POST['accountEmployeeCountExcludes'] : '';

// Founded Year filter
$accountFoundedYear = isset($_POST['accountFoundedYear']) ? $_POST['accountFoundedYear'] : '';

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

$accountNameContains = $conn->real_escape_string($accountNameContains);
$accountNameStartsWith = $conn->real_escape_string($accountNameStartsWith);
$accountNameIncludes = $conn->real_escape_string($accountNameIncludes);
$accountNameExcludes = $conn->real_escape_string($accountNameExcludes);

$accountWebsiteContains = $conn->real_escape_string($accountWebsiteContains);
$accountWebsiteStartsWith = $conn->real_escape_string($accountWebsiteStartsWith);
$accountWebsiteIncludes = $conn->real_escape_string($accountWebsiteIncludes);
$accountWebsiteExcludes = $conn->real_escape_string($accountWebsiteExcludes);

$accountIndustryContains = $conn->real_escape_string($accountIndustryContains);
$accountIndustryStartsWith = $conn->real_escape_string($accountIndustryStartsWith);
$accountIndustryIncludes = $conn->real_escape_string($accountIndustryIncludes);
$accountIndustryExcludes = $conn->real_escape_string($accountIndustryExcludes);

$accountEmployeeCountContains = $conn->real_escape_string($accountEmployeeCountContains);
$accountEmployeeCountStartsWith = $conn->real_escape_string($accountEmployeeCountStartsWith);
$accountEmployeeCountIncludes = $conn->real_escape_string($accountEmployeeCountIncludes);
$accountEmployeeCountExcludes = $conn->real_escape_string($accountEmployeeCountExcludes);

$accountFoundedYear = $conn->real_escape_string($accountFoundedYear);

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

// Apply Account Name filters
if (!empty($accountNameContains)) {
    $sql .= " AND " . applyFilter('account_name', $accountNameContains, 'contains');
}
if (!empty($accountNameStartsWith)) {
    $sql .= " AND " . applyFilter('account_name', $accountNameStartsWith, 'starts_with');
}
if (!empty($accountNameIncludes)) {
    $sql .= " AND " . applyFilter('account_name', $accountNameIncludes, 'include');
}
if (!empty($accountNameExcludes)) {
    $sql .= " AND " . applyFilter('account_name', $accountNameExcludes, 'exclude');
}

// Apply Website filters
if (!empty($accountWebsiteContains)) {
    $sql .= " AND " . applyFilter('account_website', $accountWebsiteContains, 'contains');
}
if (!empty($accountWebsiteStartsWith)) {
    $sql .= " AND " . applyFilter('account_website', $accountWebsiteStartsWith, 'starts_with');
}
if (!empty($accountWebsiteIncludes)) {
    $sql .= " AND " . applyFilter('account_website', $accountWebsiteIncludes, 'include');
}
if (!empty($accountWebsiteExcludes)) {
    $sql .= " AND " . applyFilter('account_website', $accountWebsiteExcludes, 'exclude');
}

// Apply Industry filters
if (!empty($accountIndustryContains)) {
    $sql .= " AND " . applyFilter('account_industry', $accountIndustryContains, 'contains');
}
if (!empty($accountIndustryStartsWith)) {
    $sql .= " AND " . applyFilter('account_industry', $accountIndustryStartsWith, 'starts_with');
}
if (!empty($accountIndustryIncludes)) {
    $sql .= " AND " . applyFilter('account_industry', $accountIndustryIncludes, 'include');
}
if (!empty($accountIndustryExcludes)) {
    $sql .= " AND " . applyFilter('account_industry', $accountIndustryExcludes, 'exclude');
}

// Apply Employee Count filters
if (!empty($accountEmployeeCountContains)) {
    $sql .= " AND " . applyFilter('account_employee_count_range', $accountEmployeeCountContains, 'contains');
}
if (!empty($accountEmployeeCountStartsWith)) {
    $sql .= " AND " . applyFilter('account_employee_count_range', $accountEmployeeCountStartsWith, 'starts_with');
}
if (!empty($accountEmployeeCountIncludes)) {
    $sql .= " AND " . applyFilter('account_employee_count_range', $accountEmployeeCountIncludes, 'include');
}
if (!empty($accountEmployeeCountExcludes)) {
    $sql .= " AND " . applyFilter('account_employee_count_range', $accountEmployeeCountExcludes, 'exclude');
}

// Apply Founded Year filter
if (!empty($accountFoundedYear)) {
    $sql .= " AND `account_founded_year` = '$accountFoundedYear'";
}

// Apply Country filter
if (!empty($country)) {
    $countryConditions = [];
    foreach ($country as $c) {
        $escapedCountry = $conn->real_escape_string($c);
        $countryConditions[] = "`account_country` = '$escapedCountry'";
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
        $cityConditions[] = "`account_city` = '$escapedCity'";
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
