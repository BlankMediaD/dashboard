<?php
session_start();

// Redis configuration
$redisHost = "localhost";
$redisPort = 6379;
$redisPassword = null; // Set this if your Redis server requires authentication
$redisTimeout = 2.5;

// Initialize Redis connection
$redis = null;
$redisConnected = false;
$cacheMemoryUsage = "N/A";
$cacheMemoryPeak = "N/A";

try {
    $redis = new Redis();
    $connected = $redis->connect($redisHost, $redisPort, $redisTimeout);
    if ($connected && $redisPassword) {
        $redis->auth($redisPassword);
    }
    if ($connected) {
        $redisConnected = true;
        $info = $redis->info();
        $cacheMemoryUsage = formatBytes($info["used_memory"]);
        $cacheMemoryPeak = formatBytes($info["used_memory_peak"]);
    }
} catch (Exception $e) {
    // Redis connection failed, continue without caching
}

// Format bytes to human-readable format
function formatBytes($bytes, $precision = 2)
{
    $units = ["B", "KB", "MB", "GB", "TB"];
    $bytes = max($bytes, 0);
    $pow = floor(($bytes ? log($bytes) : 0) / log(1024));
    $pow = min($pow, count($units) - 1);
    $bytes /= 1 << 10 * $pow;
    return round($bytes, $precision) . " " . $units[$pow];
}

// Handle cache flush if requested
if (isset($_POST["flush_cache"]) && $redisConnected) {
    // Delete all keys with the tuesday_ prefix
    $keys = $redis->keys("tuesday_*");
    if (!empty($keys)) {
        foreach ($keys as $key) {
            $redis->del($key);
        }
    }
    // Redirect to prevent form resubmission
    header("Location: " . $_SERVER["PHP_SELF"]);
    exit();
}
if (!isset($_SESSION['authenticated']) || $_SESSION['authenticated'] !== true) {
    // If user is not authenticated, redirect to password.php
    header("Location: password.php");
    exit;
}
?>
<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Tuesday Company Dashboard</title>
    <link rel="icon" href="favicon.png" type="image/png">
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.1/css/jquery.dataTables.min.css">
    <link rel="stylesheet" href="https://code.jquery.com/ui/1.12.1/themes/base/jquery-ui.css">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/flatpickr.min.css">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.3/css/all.min.css">

    <style>
        * {
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            margin: 0;
            padding: 0;
            background-color: #f0f2f5;
            color: var(--text-color);
        }

        :root {
            
            /* Grey-blue color palette */
            --primary-color: #4a5d7e;
            --text-color: #2c3e50;
            --border-color: #d1d9e6;
            --hover-color: #e6eaf0;
            --tag-bg-color: #e6eaf0;
            --tag-text-color: #4a5d7e;
            --secondary-color: #6c7a8c;
            --light-grey: #f0f2f5;
            --medium-grey: #a9b6c8;
            --dark-grey: #4a5566;
            --highlight-color: #5d7299;
            --danger-color: #dc3545;
        }

        .container {
            width: 100%;
            max-width: 1400px;
            margin: 0 auto;
            padding: 0 20px;
        }

        .dashboard-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 20px 0;
            border-bottom: 1px solid var(--border-color);
            margin-bottom: 20px;
        }

        .dashboard-title {
            font-size: 24px;
            font-weight: 600;
            margin: 0;
        }

        .dashboard-actions {
            display: flex;
            gap: 10px;
        }

        .dashboard-card {
            background-color: #fff;
            border-radius: 0px;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
            padding: 20px;
            margin-bottom: 20px;
        }

        /* Cache stats styles */
        .cache-stats {
            display: flex;
            gap: 15px;
            margin-right: 15px;
        }

        .cache-stat {
            background-color: var(--tag-bg-color);
            color: var(--tag-text-color);
            padding: 8px 12px;
            border-radius: 4px;
            font-size: 13px;
            display: flex;
            flex-direction: column;
            align-items: center;
        }

        .cache-stat-value {
            font-weight: 600;
            font-size: 14px;
        }

        .cache-stat-label {
            font-size: 11px;
            opacity: 0.8;
        }

        /* Flush cache button */
        .flush-cache-btn {
            background-color: var(--danger-color);
            color: white;
            border: none;
            border-radius: 4px;
            padding: 8px 12px;
            font-size: 13px;
            font-weight: 500;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 5px;
            transition: background-color 0.2s;
        }

        .flush-cache-btn:hover {
            background-color: #c82333;
        }

        /* Filter section styles */
        .filter-section {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-bottom: 20px;
            align-items: center;
            position: relative;
        }

        /* Search input */
        .search-container {
            position: relative;
            min-width: 240px;
        }

        .search-input {
            width: 100%;
            padding: 10px 15px 10px 35px;
            border: 1px solid var(--border-color);
            border-radius: 0px;
            font-size: 14px;
            color: var(--text-color);
            transition: all 0.2s;
        }

        .search-input:focus {
            outline: none;
            border-color: var(--primary-color);
            box-shadow: 0 0 0 2px rgba(74, 108, 247, 0.1);
        }

        .search-icon {
            position: absolute;
            left: 12px;
            top: 50%;
            transform: translateY(-50%);
            color: var(--secondary-color);
        }

        /* Filter buttons */
        .filter-button {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 10px 15px;
            border: 1px solid var(--border-color);
            border-radius: 0px;
            background-color: white;
            font-size: 14px;
            color: var(--text-color);
            cursor: pointer;
            min-width: 120px;
            transition: all 0.2s;
        }

        .filter-button:hover {
            border-color: var(--primary-color);
            background-color: var(--hover-color);
        }

        .filter-button i {
            font-size: 12px;
            margin-left: 5px;
        }

        /* Add filter button */
        .add-filter-button {
            display: flex;
            align-items: center;
            padding: 10px 15px;
            border: 1px solid var(--border-color);
            border-radius: 0px;
            background-color: white;
            font-size: 14px;
            color: var(--primary-color);
            cursor: pointer;
            font-weight: 500;
            transition: all 0.2s;
        }

        .add-filter-button:hover {
            background-color: var(--hover-color);
            border-color: var(--primary-color);
        }

        .add-filter-button i {
            margin-right: 5px;
        }

        /* Clear filters button */
        .clear-filters-button {
            display: flex;
            align-items: center;
            padding: 10px 15px;
            border: 1px solid var(--border-color);
            border-radius: 0px;
            background-color: white;
            font-size: 14px;
            color: var(--danger-color);
            cursor: pointer;
            font-weight: 500;
            transition: all 0.2s;
        }

        .clear-filters-button:hover {
            background-color: #fff5f5;
            border-color: var(--danger-color);
        }

        .clear-filters-button i {
            margin-right: 5px;
        }

        /* Dropdown menu */
        .dropdown {
            position: relative;
            display: inline-block;
        }

        .dropdown-content {
            display: none;
            position: absolute;
            right: 0;
            top: 100%;
            margin-top: 5px;
            background-color: white;
            min-width: 280px;
            max-width: 350px;
            max-height: 400px;
            overflow-y: auto;
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1);
            border-radius: 4px;
            padding: 15px;
            z-index: 10;
        }

        .dropdown-content.show {
            display: block;
        }

        /* Filter menu */
        .filter-menu {
            position: absolute;
            right: 0;
            top: 100%;
            margin-top: 5px;
            background-color: white;
            min-width: 200px;
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1);
            border-radius: 0px;
            padding: 10px;
            z-index: 10;
            display: none;
        }

        .filter-menu.show {
            display: block;
        }

        /* Checkbox styles */
        .checkbox-item {
            display: flex;
            align-items: center;
            padding: 8px 5px;
            cursor: pointer;
            border-radius: 0px;
            transition: background-color 0.2s;
        }

        .checkbox-item:hover {
            background-color: var(--hover-color);
        }

        .checkbox-item input[type="checkbox"],
        .checkbox-item input[type="radio"] {
            margin-right: 10px;
        }

        /* Subfilter styles */
        .subfilter-container {
            display: flex;
            flex-direction: column;
            gap: 10px;
            margin-bottom: 10px;
        }

        .subfilter-row {
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .subfilter-label {
            font-size: 14px;
            width: 100px;
        }

        .subfilter-input {
            flex: 1;
            padding: 8px 12px;
            border: 1px solid var(--border-color);
            border-radius: 0px;
            font-size: 14px;
        }

        .subfilter-apply {
            width: 100%;
            padding: 10px;
            background-color: var(--primary-color);
            color: white;
            border: none;
            border-radius: 0px;
            font-size: 14px;
            font-weight: 500;
            cursor: pointer;
            margin-top: 10px;
        }

        .subfilter-apply:hover {
            background-color: var(--highlight-color);
        }

        /* Filter search input */
        .filter-search {
            width: 100%;
            padding: 8px 12px;
            border: 1px solid var(--border-color);
            border-radius: 0px;
            font-size: 14px;
            margin-bottom: 10px;
        }

        .filter-search:focus {
            outline: none;
            border-color: var(--primary-color);
        }

        /* Options container */
        .options-container {
            max-height: 250px;
            overflow-y: auto;
            margin-bottom: 10px;
            border: 1px solid var(--border-color);
            border-radius: 0px;
            padding: 5px;
        }

        /* Active filters */
        .active-filters {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-top: 15px;
            margin-bottom: 15px;
        }

        .filter-tag {
            display: inline-flex;
            align-items: center;
            background-color: var(--tag-bg-color);
            color: var(--tag-text-color);
            padding: 5px 12px;
            border-radius: 0px;
            font-size: 13px;
        }

        .filter-tag .remove-filter {
            margin-left: 8px;
            cursor: pointer;
            color: var(--secondary-color);
        }

        /* Table styles */
        .table-wrapper {
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
            border-radius: 0px;
            background: white;
            padding: 1px;
        }

        table.dataTable {
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
            background-color: white;
        }

        table.dataTable thead th {
            background-color: white;
            color: var(--secondary-color);
            font-weight: 500;
            font-size: 12px;
            text-transform: uppercase;
            padding: 15px;
            border-bottom: 1px solid var(--border-color);
        }

        table.dataTable tbody td {
            background-color: white;
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
            font-size: 14px;
            max-width: 200px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: normal;
            word-wrap: break-word;
        }

        /* Add subtle rounded corners to the table */
        table.dataTable thead tr:first-child th:first-child {
            border-top-left-radius: 8px;
        }

        table.dataTable thead tr:first-child th:last-child {
            border-top-right-radius: 8px;
        }

        table.dataTable tbody tr:last-child td:first-child {
            border-bottom-left-radius: 8px;
        }

        table.dataTable tbody tr:last-child td:last-child {
            border-bottom-right-radius: 8px;
        }

        /* Hover effect */
        table.dataTable tbody tr:hover td {
            background-color: var(--hover-color);
        }

        /* Remove default DataTables borders */
        table.dataTable.no-footer {
            border-bottom: none;
        }

        table.dataTable thead th {
            border-bottom: 1px solid var(--border-color) !important;
        }

        /* Modal styles */
        .modal {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0, 0, 0, 0.5);
            z-index: 1000;
            overflow-y: auto;
            animation: fadeIn 0.2s;
        }

        .modal-content {
            background-color: white;
            width: 90%;
            max-width: 500px;
            margin: 80px auto;
            border-radius: 0px;
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1);
            animation: slideUp 0.3s;
        }

        .modal-header {
            padding: 15px 20px;
            border-bottom: 1px solid var(--border-color);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .modal-title {
            font-size: 18px;
            font-weight: 600;
            color: var(--text-color);
            margin: 0;
        }

        .modal-body {
            padding: 20px;
            max-height: 400px;
            overflow-y: auto;
        }

        .modal-footer {
            padding: 15px 20px;
            border-top: 1px solid var(--border-color);
            display: flex;
            justify-content: flex-end;
            gap: 10px;
        }

        .close-modal {
            font-size: 24px;
            cursor: pointer;
            color: var(--secondary-color);
        }

        /* Button styles */
        .btn {
            padding: 8px 16px;
            border-radius: 0px;
            font-size: 14px;
            font-weight: 500;
            cursor: pointer;
            border: none;
            transition: all 0.2s;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 5px;
        }

        .btn-primary {
            background-color: var(--primary-color);
            color: white;
        }

        .btn-primary:hover {
            background-color: var(--highlight-color);
        }

        .btn-outline {
            background-color: transparent;
            border: 1px solid var(--border-color);
            color: var(--text-color);
        }

        .btn-outline:hover {
            background-color: var(--hover-color);
        }

        /* Expanded row styles */
        .expanded-row {
            background-color: var(--light-grey);
            padding: 15px;
            border-radius: 0px;
            margin: 10px 0;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
        }

        .expanded-row-item {
            border: 1px solid var(--border-color);
            padding: 10px;
            margin-bottom: 8px;
            border-radius: 0px;
            background-color: white;
        }

        .expanded-row-item:last-child {
            margin-bottom: 0;
        }

        .expanded-row-item strong {
            font-weight: 600;
            color: var(--primary-color);
            margin-right: 5px;
        }

        .expanded-row-item span {
            font-weight: normal;
            color: var(--secondary-color);
        }

        /* Expand icon */
        .expand-icon {
            cursor: pointer;
            color: var(--primary-color);
            font-size: 16px;
            margin-right: 8px;
            display: inline-block;
            transition: transform 0.2s;
        }

        tr.shown .expand-icon {
            transform: rotate(90deg);
        }

        /* Hide DataTables info */
        .dataTables_info,
        .dataTables_length {
            display: none !important;
        }

        /* Export popup styles */
        #exportPopupOverlay {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0, 0, 0, 0.5);
            z-index: 1000;
        }

        #exportPopup {
            display: none;
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            background-color: white;
            width: 600px;
            max-width: 90%;
            border-radius: 0px;
            box-shadow: 0 5px 25px rgba(0, 0, 0, 0.15);
            z-index: 1001;
            padding: 0;
            overflow: hidden;
        }

        .export-popup-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 20px 24px;
            background-color: var(--light-grey);
            border-bottom: 1px solid var(--border-color);
        }

        .export-popup-title {
            font-size: 18px;
            font-weight: 600;
            margin: 0;
            color: var(--text-color);
        }

        .export-popup-body {
            padding: 24px;
        }

        .export-section {
            margin-bottom: 24px;
        }

        .export-section-title {
            font-size: 16px;
            font-weight: 600;
            margin: 0 0 12px 0;
            color: var(--text-color);
        }

        .export-options {
            display: flex;
            flex-direction: column;
            gap: 20px;
        }

        .row-range-inputs {
            display: flex;
            gap: 16px;
            margin-top: 12px;
        }

        .row-range-inputs>div {
            flex: 1;
        }

        .input-group {
            margin-bottom: 12px;
        }

        .input-group label {
            display: block;
            margin-bottom: 6px;
            font-size: 14px;
            color: var(--secondary-color);
        }

        .input-control {
            width: 100%;
            padding: 10px 12px;
            border: 1px solid var(--border-color);
            border-radius: 0px;
            font-size: 14px;
            transition: all 0.2s;
        }

        .input-control:focus {
            outline: none;
            border-color: var(--primary-color);
            box-shadow: 0 0 0 2px rgba(74, 108, 247, 0.1);
        }

        .column-selection {
            border: 1px solid var(--border-color);
            border-radius: 0px;
            max-height: 200px;
            overflow-y: auto;
            padding: 8px;
        }

        .column-group {
            margin-bottom: 12px;
        }

        .column-group-title {
            font-weight: 600;
            margin-bottom: 8px;
            font-size: 14px;
            color: var(--text-color);
        }

        .column-options {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 8px;
        }

        .column-checkbox {
            display: flex;
            align-items: center;
            padding: 6px 8px;
            border-radius: 0px;
            transition: background-color 0.2s;
        }

        .column-checkbox:hover {
            background-color: var(--hover-color);
        }

        .column-checkbox input {
            margin-right: 8px;
        }

        .export-actions {
            display: flex;
            justify-content: space-between;
            margin-top: 24px;
            padding-top: 20px;
            border-top: 1px solid var(--border-color);
        }

        .export-btn {
            padding: 12px 24px;
            background-color: var(--primary-color);
            color: white;
            border: none;
            border-radius: 0px;
            font-weight: 500;
            font-size: 15px;
            cursor: pointer;
            transition: background-color 0.2s;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .export-btn:hover {
            background-color: var(--highlight-color);
        }

        .export-btn i {
            font-size: 16px;
        }

        .export-btn.btn-outline {
            background-color: transparent;
            border: 1px solid var(--border-color);
            color: var(--text-color);
        }

        .export-btn.btn-outline:hover {
            background-color: var(--hover-color);
        }

        .select-all-columns {
            margin-bottom: 12px;
            padding: 8px;
            background-color: var(--light-grey);
            border-radius: 0px;
            display: flex;
            align-items: center;
        }

        .select-all-columns input {
            margin-right: 8px;
        }

        /* Responsive styles */
        @media (max-width: 768px) {
            .filter-section {
                flex-direction: column;
                align-items: stretch;
            }

            .search-container {
                width: 100%;
            }

            .filter-button,
            .add-filter-button,
            .clear-filters-button {
                width: 100%;
            }

            .dropdown-content,
            .filter-menu {
                width: 100%;
                position: static;
                margin-top: 10px;
            }

            .column-options {
                grid-template-columns: 1fr;
            }

            .export-actions {
                flex-direction: column;
                gap: 12px;
            }

            .export-btn {
                width: 100%;
            }
        }

        /* DataTables-specific styles */
        .dataTables_wrapper .dataTables_paginate {
            margin-top: 20px;
            text-align: center;
        }

        .dataTables_wrapper .dataTables_paginate .paginate_button {
            padding: 8px 12px;
            margin: 0 4px;
            border: 1px solid var(--border-color);
            border-radius: 0px;
            background-color: white;
            color: var(--text-color) !important;
            cursor: pointer;
            transition: all 0.2s;
        }

        .dataTables_wrapper .dataTables_paginate .paginate_button:hover {
            background-color: var(--hover-color);
            border-color: var(--primary-color);
        }

        .dataTables_wrapper .dataTables_paginate .paginate_button.current {
            background-color: var(--primary-color);
            color: white !important;
            border-color: var(--primary-color);
        }

        .dataTables_wrapper .dataTables_paginate .paginate_button.disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }

        /* Fade In Animation */
        @keyframes fadeIn {
            0% {
                opacity: 0;
            }

            100% {
                opacity: 1;
            }
        }

        /* Slide Up Animation */
        @keyframes slideUp {
            0% {
                transform: translateY(20px);
                opacity: 0;
            }

            100% {
                transform: translateY(0);
                opacity: 1;
            }
        }

        .total-entries-counter {
            background-color: var(--tag-bg-color);
            color: var(--tag-text-color);
            padding: 16px 16px;
            border-radius: 4px;
            font-size: 14px;
            font-weight: 500;
            margin-right: 10px;
        }

        /* Entries Info Container */
        .entries-info-container {
            margin-top: 15px;
            padding: 10px 0;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .entries-info {
            color: var(--secondary-color);
            font-size: 14px;
            padding: 5px 0;
        }

        /* Entries per page dropdown styles */
        .entries-per-page {
            margin-bottom: 15px;
            font-size: 14px;
            color: var(--text-color);
        }

        .entries-per-page select {
            margin: 0 5px;
            padding: 5px 10px;
            border: 1px solid var(--border-color);
            border-radius: 4px;
            background-color: white;
        }

        /* Confirmation Modal Styles */
        #confirmationModal {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0, 0, 0, 0.5);
            z-index: 1000;
            align-items: center;
            justify-content: center;
        }

        .confirmation-content {
            background-color: white;
            width: 400px;
            max-width: 90%;
            border-radius: 8px;
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.15);
            padding: 20px;
        }

        .confirmation-title {
            font-size: 18px;
            font-weight: 600;
            margin-bottom: 15px;
            color: var(--text-color);
        }

        .confirmation-message {
            margin-bottom: 20px;
            color: var(--secondary-color);
            font-size: 14px;
            line-height: 1.5;
        }

        .confirmation-actions {
            display: flex;
            justify-content: flex-end;
            gap: 10px;
        }

        .confirm-btn {
            padding: 8px 16px;
            border-radius: 4px;
            font-size: 14px;
            font-weight: 500;
            cursor: pointer;
            border: none;
        }

        .confirm-yes {
            background-color: var(--danger-color);
            color: white;
        }

        .confirm-no {
            background-color: var(--light-grey);
            color: var(--text-color);
            border: 1px solid var(--border-color);
        }

        /* Cache Monitor Button */
        .cache-monitor-btn {
            background-color: var(--primary-color);
            color: white;
            border: none;
            border-radius: 4px;
            padding: 8px 12px;
            font-size: 13px;
            font-weight: 500;
            cursor: pointer;
            display: flex;
            align-items: center;
            gap: 5px;
            transition: background-color 0.2s;
            margin-right: 10px;
        }

        .cache-monitor-btn:hover {
            background-color: var(--highlight-color);
        }
    </style>
</head>

<body>

    <div class="container">
        <div class="dashboard-header">
            <h1 class="dashboard-title">Tuesday Company Dashboard</h1>
            <div class="dashboard-actions">
                <?php if ($redisConnected): ?>
                <div class="cache-stats">
                    <div class="cache-stat">
                        <span class="cache-stat-value"><?php echo $cacheMemoryUsage; ?></span>
                        <span class="cache-stat-label">Cache Memory</span>
                    </div>
                    <div class="cache-stat">
                        <span class="cache-stat-value"><?php echo $cacheMemoryPeak; ?></span>
                        <span class="cache-stat-label">Peak Memory</span>
                    </div>
                </div>
                <a href="cache-monitor.php" class="cache-monitor-btn">
                    <i class="fas fa-tachometer-alt"></i> Cache Monitor
                </a>
                <button id="flushCacheBtn" class="flush-cache-btn">
                    <i class="fas fa-trash-alt"></i> Flush Cache
                </button>
                <?php endif; ?>
                <div class="total-entries-counter">
                    <span id="totalEntriesCount">0</span> total entries
                </div>
                <button id="openExportPopup" class="btn btn-primary">
                    <i class="fas fa-download"></i> Export
                </button>
            </div>
        </div>

        <div class="dashboard-card">
            <!-- Filter Section -->
            <div class="filter-section">
                <!-- Search Input -->
                <div class="search-container">
                    <i class="fas fa-search search-icon"></i>
                    <input type="text" id="searchInput" class="search-input" placeholder="Search by Account Name">
                </div>

                <!-- Account Name Filter -->
                <div class="dropdown">
                    <button id="accountNameFilterBtn" class="filter-button">
                        Account Name <i class="fas fa-chevron-down"></i>
                    </button>
                    <div id="accountNameDropdown" class="dropdown-content">
                        <div class="subfilter-container">
                            <div class="subfilter-row">
                                <label class="subfilter-label">Contains</label>
                                <input type="text" id="account_name_contains" class="subfilter-input"
                                    placeholder="Enter Account Name">
                            </div>
                            <div class="subfilter-row">
                                <label class="subfilter-label">Starts With</label>
                                <input type="text" id="account_name_starts_with" class="subfilter-input"
                                    placeholder="Enter Account Name">
                            </div>
                            <div class="subfilter-row">
                                <label class="subfilter-label">Includes</label>
                                <input type="text" id="account_name_includes" class="subfilter-input"
                                    placeholder="Enter Account Name">
                            </div>
                            <div class="subfilter-row">
                                <label class="subfilter-label">Excludes</label>
                                <input type="text" id="account_name_excludes" class="subfilter-input"
                                    placeholder="Enter Account Name">
                            </div>
                            <button id="apply_account_name_filter" class="subfilter-apply">Apply</button>
                        </div>
                    </div>
                </div>

                <!-- Country Filter -->
                <div class="dropdown">
                    <button id="countryFilterBtn" class="filter-button">
                        Country <i class="fas fa-chevron-down"></i>
                    </button>
                    <div id="countryDropdown" class="dropdown-content">
                        <div class="subfilter-container">
                            <input type="text" id="country_search" class="filter-search" placeholder="Search countries...">
                            <div class="select-all-columns">
                                <input type="checkbox" id="selectAllCountries">
                                <label for="selectAllCountries">Select/Deselect All</label>
                            </div>
                            <div id="country_options" class="options-container">
                                <!-- Country options will be loaded here dynamically -->
                            </div>
                            <button id="apply_country_filter" class="subfilter-apply">Apply</button>
                        </div>
                    </div>
                </div>

                <!-- City Filter -->
                <div class="dropdown">
                    <button id="cityFilterBtn" class="filter-button">
                        City <i class="fas fa-chevron-down"></i>
                    </button>
                    <div id="cityDropdown" class="dropdown-content">
                        <div class="subfilter-container">
                            <input type="text" id="city_search" class="filter-search" placeholder="Search cities...">
                            <div class="select-all-columns">
                                <input type="checkbox" id="selectAllCities">
                                <label for="selectAllCities">Select/Deselect All</label>
                            </div>
                            <div id="city_options" class="options-container">
                                <!-- City options will be loaded here dynamically -->
                            </div>
                            <button id="apply_city_filter" class="subfilter-apply">Apply</button>
                        </div>
                    </div>
                </div>

                <!-- Add Filter Button -->
                <div class="dropdown">
                    <button id="addFilterBtn" class="add-filter-button">
                        <i class="fas fa-plus"></i> Filter
                    </button>
                    <div id="filterMenu" class="filter-menu">
                        <div class="checkbox-item">
                            <input type="checkbox" id="filter_website">
                            <label for="filter_website">Website</label>
                        </div>
                        <div class="checkbox-item">
                            <input type="checkbox" id="filter_industry">
                            <label for="filter_industry">Industry</label>
                        </div>
                        <div class="checkbox-item">
                            <input type="checkbox" id="filter_employee_count">
                            <label for="filter_employee_count">Employee Count</label>
                        </div>
                        <div class="checkbox-item">
                            <input type="checkbox" id="filter_founded_year">
                            <label for="filter_founded_year">Founded Year</label>
                        </div>
                    </div>
                </div>

                <!-- Clear Filters Button -->
                <button id="clearFiltersBtn" class="clear-filters-button">
                    <i class="fas fa-times"></i> Clear Filters
                </button>
            </div>

            <!-- Active Filters Display -->
            <div class="active-filters" id="activeFilters">
                <!-- Active filters will be added here dynamically -->
            </div>

            <!-- Entries per page dropdown -->
            <div class="entries-per-page">
                <label>Show
                    <select id="entriesPerPage">
                        <option value="10">10</option>
                        <option value="50">50</option>
                        <option value="100">100</option>
                    </select>
                    entries
                </label>
            </div>

            <!-- Data table -->
            <div class="table-wrapper">
                <table id="tuesdayTable" class="display" width="100%">
                    <thead>
                        <tr>
                            <th>Account Name</th>
                            <th>Account Website</th>
                            <th>Account Industry</th>
                            <th>Employee Count Range</th>
                        </tr>
                    </thead>
                    <tbody>
                        <!-- Data will be populated here by DataTables -->
                    </tbody>
                </table>
                <div class="entries-info-container">
                    <div id="entriesInfo" class="entries-info">
                        0 entries (filtered from 0 total entries)
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- Export Popup -->
    <div id="exportPopupOverlay"></div>
    <div id="exportPopup">
        <div class="export-popup-header">
            <h3 class="export-popup-title">Export Data</h3>
            <span class="close-modal" id="closeExportPopup">&times;</span>
        </div>
        <div class="export-popup-body">
            <div class="export-options">
                <!-- Column Selection Section -->
                <div class="export-section">
                    <h4 class="export-section-title">Select Columns</h4>
                    <div class="select-all-columns">
                        <input type="checkbox" id="selectAllColumns" checked>
                        <label for="selectAllColumns">Select/Deselect All Columns</label>
                    </div>
                    <div class="column-selection">
                        <div class="column-group">
                            <div class="column-group-title">Basic Information</div>
                            <div class="column-options">
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_id" checked> Account ID
                                </label>
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_name" checked> Account Name
                                </label>
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_industry" checked> Industry
                                </label>
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_website" checked> Website
                                </label>
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_employee_count_range" checked> Employee Count Range
                                </label>
                            </div>
                        </div>

                        <div class="column-group">
                            <div class="column-group-title">Location Details</div>
                            <div class="column-options">
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_city" checked> City
                                </label>
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_country" checked> Country
                                </label>
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_state" checked> State
                                </label>
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_street" checked> Street
                                </label>
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_postcode" checked> Postcode
                                </label>
                            </div>
                        </div>

                        <div class="column-group">
                            <div class="column-group-title">Company Details</div>
                            <div class="column-options">
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_founded_year" checked> Founded Year
                                </label>
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_specialties" checked> Specialties
                                </label>
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_about_us" checked> About Us
                                </label>
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_primary_domain" checked> Primary Domain
                                </label>
                            </div>
                        </div>

                        <div class="column-group">
                            <div class="column-group-title">Additional Information</div>
                            <div class="column-options">
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_linkedin_url" checked> LinkedIn URL
                                </label>
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_employee_profiles_on_linkedin" checked> LinkedIn Profiles
                                </label>
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_has_cio" checked> Has CIO
                                </label>
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_has_ciso" checked> Has CISO
                                </label>
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_has_mobile_app" checked> Has Mobile App
                                </label>
                                <label class="column-checkbox">
                                    <input type="checkbox" name="export_column" value="account_has_web_app" checked> Has Web App
                                </label>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Row Range Section -->
                <div class="export-section">
                    <h4 class="export-section-title">Row Range</h4>
                    <div class="row-range-inputs">
                        <div class="input-group">
                            <label for="exportStartRow">Start Row:</label>
                            <input type="number" id="exportStartRow" class="input-control" placeholder="e.g., 1" min="1"
                                value="1">
                        </div>
                        <div class="input-group">
                            <label for="exportEndRow">End Row:</label>
                            <input type="number" id="exportEndRow" class="input-control" placeholder="e.g., 100" min="1"
                                value="100">
                        </div>
                    </div>
                    <div class="input-group">
                        <label for="exportLimit">Or limit to first:</label>
                        <input type="number" id="exportLimit" class="input-control" placeholder="e.g., 500 records"
                            min="1">
                    </div>
                </div>
            </div>

            <div class="export-actions">
                <button id="cancelExport" class="export-btn btn-outline">
                    <i class="fas fa-times"></i> Cancel
                </button>
                <button id="exportCsvBtn" class="export-btn">
                    <i class="fas fa-file-csv"></i> Export as CSV
                </button>
            </div>
        </div>
    </div>
    
    <!-- Confirmation Modal for Flush Cache -->
    <div id="confirmationModal">
        <div class="confirmation-content">
            <div class="confirmation-title">Confirm Cache Flush</div>
            <div class="confirmation-message">
                Are you sure you want to flush the cache? This will clear all cached data and may temporarily slow down the application.
            </div>
            <div class="confirmation-actions">
                <form method="post">
                    <input type="hidden" name="flush_cache" value="1">
                    <button type="submit" class="confirm-btn confirm-yes">Yes, Flush Cache</button>
                </form>
                <button class="confirm-btn confirm-no" id="cancelFlushCache">Cancel</button>
            </div>
        </div>
    </div>

    <!-- JavaScript Libraries -->
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script src="https://code.jquery.com/ui/1.12.1/jquery-ui.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.1/js/jquery.dataTables.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>

    <!-- Custom JavaScript -->
    <script>
        $(document).ready(function () {
            // Variables to track current page and filters
            let currentPage = 1;
            const recordsPerPage = 10;

            // Object to store active filters
            let activeFilters = {
                searchValue: '',
                account_name: {
                    contains: '',
                    starts_with: '',
                    includes: '',
                    excludes: ''
                },
                account_website: {
                    contains: '',
                    starts_with: '',
                    includes: '',
                    excludes: ''
                },
                account_industry: {
                    contains: '',
                    starts_with: '',
                    includes: '',
                    excludes: ''
                },
                account_employee_count_range: {
                    contains: '',
                    starts_with: '',
                    includes: '',
                    excludes: ''
                },
                account_founded_year: '',
                country: [],
                city: []
            };

            // Fixed columns for display in the table
            const fixedDisplayColumns = ['account_name', 'account_website', 'account_industry', 'account_employee_count_range'];

            // Columns selected for export (initially same as display columns)
            let exportColumns = [...fixedDisplayColumns];

            let activeFilterButtons = ['account_name', 'account_country', 'account_city']; // Track which filter buttons are active

            // Initialize the data table
            let table = $('#tuesdayTable').DataTable({
                "processing": true,
                "serverSide": true,
                "searching": false,
                "info": false,
                "lengthChange": false,
                "pageLength": recordsPerPage,
                "pagingType": "simple_numbers",
                "language": {
                    "paginate": {
                        "first": '<i class="fas fa-angle-double-left"></i>',
                        "previous": '<i class="fas fa-angle-left"></i>',
                        "next": '<i class="fas fa-angle-right"></i>',
                        "last": '<i class="fas fa-angle-double-right"></i>'
                    }
                },
                "ajax": {
                    "url": "data.php",  // Pointing to your PHP script
                    "type": "POST",
                    "data": function (d) {
                        // Pass the search values and operators to the PHP script
                        d.searchValue = activeFilters.searchValue;

                        // Account Name filters
                        d.accountNameContains = activeFilters.account_name.contains;
                        d.accountNameStartsWith = activeFilters.account_name.starts_with;
                        d.accountNameIncludes = activeFilters.account_name.includes;
                        d.accountNameExcludes = activeFilters.account_name.excludes;

                        // Website filters
                        d.accountWebsiteContains = activeFilters.account_website.contains;
                        d.accountWebsiteStartsWith = activeFilters.account_website.starts_with;
                        d.accountWebsiteIncludes = activeFilters.account_website.includes;
                        d.accountWebsiteExcludes = activeFilters.account_website.excludes;

                        // Industry filters
                        d.accountIndustryContains = activeFilters.account_industry.contains;
                        d.accountIndustryStartsWith = activeFilters.account_industry.starts_with;
                        d.accountIndustryIncludes = activeFilters.account_industry.includes;
                        d.accountIndustryExcludes = activeFilters.account_industry.excludes;

                        // Employee Count filters
                        d.accountEmployeeCountContains = activeFilters.account_employee_count_range.contains;
                        d.accountEmployeeCountStartsWith = activeFilters.account_employee_count_range.starts_with;
                        d.accountEmployeeCountIncludes = activeFilters.account_employee_count_range.includes;
                        d.accountEmployeeCountExcludes = activeFilters.account_employee_count_range.excludes;

                        // Founded Year filter
                        d.accountFoundedYear = activeFilters.account_founded_year;

                        // Country filter
                        d.country = activeFilters.country;

                        // City filter
                        d.city = activeFilters.city;

                        d.page = Math.floor(d.start / d.length) + 1;  // Add page number for server-side pagination
                        d.limit = d.length;  // Add limit for pagination
                    },
                    "dataSrc": function (json) {
                        // Update the total entries counter
                        $('#totalEntriesCount').text(json.totalEntries.toLocaleString());

                        // Update the entries info at the bottom
                        const filteredEntries = json.recordsFiltered.toLocaleString();
                        const totalEntries = json.recordsTotal.toLocaleString();
                        $('#entriesInfo').text(`${filteredEntries} entries (filtered from ${totalEntries} total entries)`);

                        return json.data;
                    },
                    "error": function (xhr, error, code) {
                        // Show the error on the table if the AJAX fails
                        console.error("Error fetching data:", xhr.responseText);
                        $('#tuesdayTable tbody').html('<tr><td colspan="4" style="text-align: center;">Error loading data. Please try again.</td></tr>');
                    }
                },
                "columns": [
                    {
                        "data": "account_name",
                        "render": function (data, type, row) {
                            return (data && data.toLowerCase() !== 'nan' && data !== null) ?
                                '<span class="expand-icon">▸</span>' + data :
                                '<span class="expand-icon">▸</span> Not Found';
                        }
                    },
                    {
                        "data": "account_website",
                        "render": function (data, type, row) {
                            if (data && data.toLowerCase() !== 'nan' && data !== null) {
                                return `<a href="${data}" target="_blank">${data}</a>`;
                            }
                            return 'Not Found';
                        }
                    },
                    {
                        "data": "account_industry",
                        "render": function (data, type, row) {
                            return (data && data.toLowerCase() !== 'nan' && data !== null) ? data : 'Not Found';
                        }
                    },
                    {
                        "data": "account_employee_count_range",
                        "render": function (data, type, row) {
                            return (data && data.toLowerCase() !== 'nan' && data !== null) ? data : 'Not Found';
                        }
                    }
                ]
            });

            // Handle entries per page change
            $('#entriesPerPage').change(function() {
                const newLength = parseInt($(this).val());
                table.page.len(newLength).draw();
            });

            // Load countries for the dropdown
            $.ajax({
                url: 'data.php',
                type: 'POST',
                data: { action: 'getCountries' },
                dataType: 'json',
                success: function (response) {
                    if (response.success && response.countries) {
                        let html = '';
                        response.countries.forEach(function (country) {
                            html += `
                            <div class="checkbox-item country-option">
                                <input type="checkbox" id="country_${country.replace(/\s+/g, '_')}" value="${country}">
                                <label for="country_${country.replace(/\s+/g, '_')}">${country}</label>
                            </div>`;
                        });
                        $('#country_options').html(html);

                        // Add search functionality for countries
                        $('#country_search').on('input', function () {
                            const searchTerm = $(this).val().toLowerCase();
                            $('.country-option').each(function () {
                                const countryName = $(this).find('label').text().toLowerCase();
                                if (countryName.includes(searchTerm)) {
                                    $(this).show();
                                } else {
                                    $(this).hide();
                                }
                            });
                        });

                        // Select all countries functionality
                        $('#selectAllCountries').change(function() {
                            const isChecked = $(this).prop('checked');
                            $('.country-option input[type="checkbox"]').prop('checked', isChecked);
                        });
                    } else {
                        $('#country_options').html('<div>No countries found</div>');
                    }
                },
                error: function () {
                    $('#country_options').html('<div>Error loading countries</div>');
                }
            });

            // Load cities for the dropdown
            $.ajax({
                url: 'data.php',
                type: 'POST',
                data: { action: 'getCities' },
                dataType: 'json',
                success: function (response) {
                    if (response.success && response.cities) {
                        let html = '';
                        response.cities.forEach(function (city) {
                            html += `
                            <div class="checkbox-item city-option">
                                <input type="checkbox" id="city_${city.replace(/\s+/g, '_')}" value="${city}">
                                <label for="city_${city.replace(/\s+/g, '_')}">${city}</label>
                            </div>`;
                        });
                        $('#city_options').html(html);

                        // Add search functionality for cities
                        $('#city_search').on('input', function () {
                            const searchTerm = $(this).val().toLowerCase();
                            $('.city-option').each(function () {
                                const cityName = $(this).find('label').text().toLowerCase();
                                if (cityName.includes(searchTerm)) {
                                    $(this).show();
                                } else {
                                    $(this).hide();
                                }
                            });
                        });

                        // Select all cities functionality
                        $('#selectAllCities').change(function() {
                            const isChecked = $(this).prop('checked');
                            $('.city-option input[type="checkbox"]').prop('checked', isChecked);
                        });
                    } else {
                        $('#city_options').html('<div>No cities found</div>');
                    }
                },
                error: function () {
                    $('#city_options').html('<div>Error loading cities</div>');
                }
            });

            // Toggle dropdowns
            $('#accountNameFilterBtn').click(function (e) {
                e.stopPropagation();
                $('#accountNameDropdown').toggleClass('show');
                $('#countryDropdown, #cityDropdown, #filterMenu').removeClass('show');
            });

            $('#countryFilterBtn').click(function (e) {
                e.stopPropagation();
                $('#countryDropdown').toggleClass('show');
                $('#accountNameDropdown, #cityDropdown, #filterMenu').removeClass('show');
            });

            $('#cityFilterBtn').click(function (e) {
                e.stopPropagation();
                $('#cityDropdown').toggleClass('show');
                $('#accountNameDropdown, #countryDropdown, #filterMenu').removeClass('show');
            });

            $('#addFilterBtn').click(function (e) {
                e.stopPropagation();
                $('#filterMenu').toggleClass('show');
                $('#accountNameDropdown, #countryDropdown, #cityDropdown').removeClass('show');
            });

            // Close dropdowns when clicking outside
            $(document).click(function (e) {
                if (!$(e.target).closest('.dropdown').length) {
                    $('.dropdown-content, .filter-menu').removeClass('show');
                }
            });

            // Apply account name filter
            $('#apply_account_name_filter').click(function () {
                const contains = $('#account_name_contains').val();
                const startsWith = $('#account_name_starts_with').val();
                const includes = $('#account_name_includes').val();
                const excludes = $('#account_name_excludes').val();

                // Update active filters
                activeFilters.account_name.contains = contains;
                activeFilters.account_name.starts_with = startsWith;
                activeFilters.account_name.includes = includes;
                activeFilters.account_name.excludes = excludes;

                // Add filter tags
                updateAccountNameFilterTags();

                // Close dropdown
                $('#accountNameDropdown').removeClass('show');

                // Reload data
                table.ajax.reload();
            });

            // Apply country filter
            $('#apply_country_filter').click(function () {
                // Get selected countries
                const selectedCountries = [];
                $('.country-option input[type="checkbox"]:checked').each(function () {
                    selectedCountries.push($(this).val());
                });

                // Update active filters
                activeFilters.country = selectedCountries;

                // Add filter tags for countries
                updateCountryFilterTags(selectedCountries);

                // Close dropdown
                $('#countryDropdown').removeClass('show');

                // Reload data
                table.ajax.reload();
            });

            // Apply city filter
            $('#apply_city_filter').click(function () {
                // Get selected cities
                const selectedCities = [];
                $('.city-option input[type="checkbox"]:checked').each(function () {
                    selectedCities.push($(this).val());
                });

                // Update active filters
                activeFilters.city = selectedCities;

                // Add filter tags for cities
                updateCityFilterTags(selectedCities);

                // Close dropdown
                $('#cityDropdown').removeClass('show');

                // Reload data
                table.ajax.reload();
            });

            // Handle filter checkboxes in the filter menu
            $('.checkbox-item input[id^="filter_"]').change(function () {
                const filterField = $(this).attr('id').replace('filter_', '');
                const isChecked = $(this).prop('checked');

                if (isChecked) {
                    // Add to active filter buttons
                    if (!activeFilterButtons.includes(filterField)) {
                        activeFilterButtons.push(filterField);
                        addFilterButton(filterField);
                    }
                } else {
                    // Remove from active filter buttons
                    const index = activeFilterButtons.indexOf(filterField);
                    if (index > -1) {
                        activeFilterButtons.splice(index, 1);
                        removeFilterButton(filterField);
                    }
                }

                // Add filter tag
                addFilterTag('filter', filterField);

                // Reload data
                table.ajax.reload();
            });

            // Clear all filters
            $('#clearFiltersBtn').click(function () {
                // Reset all active filters
                activeFilters = {
                    searchValue: '',
                    account_name: {
                        contains: '',
                        starts_with: '',
                        includes: '',
                        excludes: ''
                    },
                    account_website: {
                        contains: '',
                        starts_with: '',
                        includes: '',
                        excludes: ''
                    },
                    account_industry: {
                        contains: '',
                        starts_with: '',
                        includes: '',
                        excludes: ''
                    },
                    account_employee_count_range: {
                        contains: '',
                        starts_with: '',
                        includes: '',
                        excludes: ''
                    },
                    account_founded_year: '',
                    country: [],
                    city: []
                };

                // Clear search input
                $('#searchInput').val('');

                // Clear all filter inputs
                $('#account_name_contains, #account_name_starts_with, #account_name_includes, #account_name_excludes').val('');
                $('#country_options input[type="checkbox"]').prop('checked', false);
                $('#city_options input[type="checkbox"]').prop('checked', false);

                // Clear all filter tags
                $('#activeFilters').empty();

                // Reload data
                table.ajax.reload();
            });

            // Add filter button to UI
            function addFilterButton(field) {
                // Create filter button HTML
                let buttonHtml = '';
                
                if (field === 'founded_year') {
                    // Special handling for year filter
                    buttonHtml = `
                        <div class="dropdown" id="dropdown_${field}">
                            <button id="${field}FilterBtn" class="filter-button">
                                ${formatFilterName(field)} <i class="fas fa-chevron-down"></i>
                            </button>
                            <div id="${field}Dropdown" class="dropdown-content">
                                <div class="subfilter-container">
                                    <div class="subfilter-row">
                                        <label class="subfilter-label">Year</label>
                                        <input type="number" id="${field}_year" class="year-picker" placeholder="e.g., 2020" min="1800" max="2030">
                                    </div>
                                    <button id="apply_${field}_filter" class="subfilter-apply">Apply</button>
                                </div>
                            </div>
                        </div>
                    `;
                } else {
                    // Standard filter with text inputs
                    buttonHtml = `
                        <div class="dropdown" id="dropdown_${field}">
                            <button id="${field}FilterBtn" class="filter-button">
                                ${formatFilterName(field)} <i class="fas fa-chevron-down"></i>
                            </button>
                            <div id="${field}Dropdown" class="dropdown-content">
                                <div class="subfilter-container">
                                    <div class="subfilter-row">
                                        <label class="subfilter-label">Contains</label>
                                        <input type="text" id="${field}_contains" class="subfilter-input" placeholder="Enter value">
                                    </div>
                                    <div class="subfilter-row">
                                        <label class="subfilter-label">Starts With</label>
                                        <input type="text" id="${field}_starts_with" class="subfilter-input" placeholder="Enter value">
                                    </div>
                                    <div class="subfilter-row">
                                        <label class="subfilter-label">Includes</label>
                                        <input type="text" id="${field}_includes" class="subfilter-input" placeholder="Enter value">
                                    </div>
                                    <div class="subfilter-row">
                                        <label class="subfilter-label">Excludes</label>
                                        <input type="text" id="${field}_excludes" class="subfilter-input" placeholder="Enter value">
                                    </div>
                                    <button id="apply_${field}_filter" class="subfilter-apply">Apply</button>
                                </div>
                            </div>
                        </div>
                    `;
                }

                // Insert before the add filter button
                $(buttonHtml).insertBefore($('#addFilterBtn').parent());

                // Add event listeners
                $(`#${field}FilterBtn`).click(function (e) {
                    e.stopPropagation();
                    $(`#${field}Dropdown`).toggleClass('show');
                    $('.dropdown-content').not(`#${field}Dropdown`).removeClass('show');
                    $('#filterMenu').removeClass('show');
                });

                if (field === 'founded_year') {
                    // Apply founded year filter
                    $('#apply_founded_year_filter').click(function () {
                        const year = $('#founded_year_year').val();
                        
                        // Update active filters
                        activeFilters.account_founded_year = year;
                        
                        // Add filter tags
                        if (year) {
                            addFieldFilterTag('founded_year', 'equals', year);
                        }
                        
                        // Close dropdown
                        $('#founded_year_dropdown').removeClass('show');
                        
                        // Reload data
                        table.ajax.reload();
                    });
                } else {
                    $(`#apply_${field}_filter`).click(function () {
                        const contains = $(`#${field}_contains`).val();
                        const startsWith = $(`#${field}_starts_with`).val();
                        const includes = $(`#${field}_includes`).val();
                        const excludes = $(`#${field}_excludes`).val();

                        // Set the appropriate filter values based on field
                        if (field === 'website') {
                            activeFilters.account_website.contains = contains;
                            activeFilters.account_website.starts_with = startsWith;
                            activeFilters.account_website.includes = includes;
                            activeFilters.account_website.excludes = excludes;
                        } else if (field === 'industry') {
                            activeFilters.account_industry.contains = contains;
                            activeFilters.account_industry.starts_with = startsWith;
                            activeFilters.account_industry.includes = includes;
                            activeFilters.account_industry.excludes = excludes;
                        } else if (field === 'employee_count') {
                            activeFilters.account_employee_count_range.contains = contains;
                            activeFilters.account_employee_count_range.starts_with = startsWith;
                            activeFilters.account_employee_count_range.includes = includes;
                            activeFilters.account_employee_count_range.excludes = excludes;
                        }

                        // Add filter tags
                        updateFieldFilterTags(field, contains, startsWith, includes, excludes);

                        // Close dropdown
                        $(`#${field}Dropdown`).removeClass('show');

                        // Reload data
                        table.ajax.reload();
                    });
                }
            }

            // Remove filter button from UI
            function removeFilterButton(field) {
                $(`#dropdown_${field}`).remove();

                // Remove from active filters if exists
                if (field === 'website') {
                    activeFilters.account_website = {
                        contains: '',
                        starts_with: '',
                        includes: '',
                        excludes: ''
                    };
                } else if (field === 'industry') {
                    activeFilters.account_industry = {
                        contains: '',
                        starts_with: '',
                        includes: '',
                        excludes: ''
                    };
                } else if (field === 'employee_count') {
                    activeFilters.account_employee_count_range = {
                        contains: '',
                        starts_with: '',
                        includes: '',
                        excludes: ''
                    };
                } else if (field === 'founded_year') {
                    activeFilters.account_founded_year = '';
                }

                $(`.filter-tag[data-field="${field}"]`).remove();
            }

            // Global search handler
            $('#searchInput').on('input', function () {
                const searchValue = $(this).val();
                activeFilters.searchValue = searchValue;
                table.ajax.reload();
            });

            // Update account name filter tags
            function updateAccountNameFilterTags() {
                // Remove existing account name filter tags
                $(`.filter-tag[data-field="account_name"]`).remove();

                // Add new tags for each non-empty filter
                if (activeFilters.account_name.contains) {
                    addFieldFilterTag('account_name', 'contains', activeFilters.account_name.contains);
                }
                if (activeFilters.account_name.starts_with) {
                    addFieldFilterTag('account_name', 'starts_with', activeFilters.account_name.starts_with);
                }
                if (activeFilters.account_name.includes) {
                    addFieldFilterTag('account_name', 'include', activeFilters.account_name.includes);
                }
                if (activeFilters.account_name.excludes) {
                    addFieldFilterTag('account_name', 'exclude', activeFilters.account_name.excludes);
                }
            }

            // Update country filter tags
            function updateCountryFilterTags(selectedCountries) {
                // Remove existing country filter tags
                $(`.filter-tag[data-field="country"]`).remove();
                
                // Add new tags for each selected country
                selectedCountries.forEach(function(country) {
                    const tagId = `country_${country.replace(/\s+/g, '_')}`;
                    const filterTag = `
                        <div class="filter-tag" data-field="country" data-value="${country}" id="${tagId}">
                            <span>Country: "${country}"</span>
                            <span class="remove-filter" data-field="country" data-value="${country}">
                                <i class="fas fa-times"></i>
                            </span>
                        </div>
                    `;
                    $('#activeFilters').append(filterTag);
                });
            }

            // Update city filter tags
            function updateCityFilterTags(selectedCities) {
                // Remove existing city filter tags
                $(`.filter-tag[data-field="city"]`).remove();
                
                // Add new tags for each selected city
                selectedCities.forEach(function(city) {
                    const tagId = `city_${city.replace(/\s+/g, '_')}`;
                    const filterTag = `
                        <div class="filter-tag" data-field="city" data-value="${city}" id="${tagId}">
                            <span>City: "${city}"</span>
                            <span class="remove-filter" data-field="city" data-value="${city}">
                                <i class="fas fa-times"></i>
                            </span>
                        </div>
                    `;
                    $('#activeFilters').append(filterTag);
                });
            }

            // Update field filter tags for other filters
            function updateFieldFilterTags(field, contains, startsWith, includes, excludes) {
                // Remove existing filter tags for this field
                $(`.filter-tag[data-field="${field}"]`).remove();

                // Add new tags for each non-empty filter
                if (contains) {
                    addFieldFilterTag(field, 'contains', contains);
                }
                if (startsWith) {
                    addFieldFilterTag(field, 'starts_with', startsWith);
                }
                if (includes) {
                    addFieldFilterTag(field, 'include', includes);
                }
                if (excludes) {
                    addFieldFilterTag(field, 'exclude', excludes);
                }
            }

            // Add filter tag to UI
            function addFilterTag(type, value) {
                // Remove existing tag if it exists
                $(`.filter-tag[data-type="${type}"][data-value="${value}"]`).remove();

                const displayValue = formatFilterName(value);
                const filterTag = `
                    <div class="filter-tag" data-type="${type}" data-value="${value}">
                        <span>Filter: ${displayValue}</span>
                        <span class="remove-filter" data-type="${type}" data-value="${value}">
                            <i class="fas fa-times"></i>
                        </span>
                    </div>
                `;
                $('#activeFilters').append(filterTag);
            }

            // Add field filter tag to UI
            function addFieldFilterTag(field, operator, value) {
                const displayField = formatFilterName(field);
                const displayOperator = formatOperator(operator);
                const tagId = `${field}_${operator}_${value.replace(/\s+/g, '_')}`;

                const filterTag = `
                    <div class="filter-tag" data-field="${field}" data-operator="${operator}" data-value="${value}" id="${tagId}">
                        <span>${displayField} ${displayOperator} "${value}"</span>
                        <span class="remove-filter" data-field="${field}" data-operator="${operator}" data-value="${value}">
                            <i class="fas fa-times"></i>
                        </span>
                    </div>
                `;
                $('#activeFilters').append(filterTag);
            }

            // Format filter value for display
            function formatFilterName(value) {
                return value
                    .replace(/_/g, ' ')
                    .replace(/\b\w/g, l => l.toUpperCase());
            }

            // Format operator for display
            function formatOperator(operator) {
                switch (operator) {
                    case 'contains': return 'contains';
                    case 'starts_with': return 'starts with';
                    case 'include': return 'includes';
                    case 'exclude': return 'excludes';
                    case 'equals': return 'is';
                    default: return operator;
                }
            }

            // Handle remove filter button click
            $(document).on('click', '.remove-filter', function () {
                const type = $(this).data('type');
                const value = $(this).data('value');
                const field = $(this).data('field');
                const operator = $(this).data('operator');

                if (field) {
                    // Remove field filter
                    if (field === 'account_name') {
                        if (operator === 'contains') activeFilters.account_name.contains = '';
                        if (operator === 'starts_with') activeFilters.account_name.starts_with = '';
                        if (operator === 'include') activeFilters.account_name.includes = '';
                        if (operator === 'exclude') activeFilters.account_name.excludes = '';
                    } else if (field === 'website') {
                        if (operator === 'contains') activeFilters.account_website.contains = '';
                        if (operator === 'starts_with') activeFilters.account_website.starts_with = '';
                        if (operator === 'include') activeFilters.account_website.includes = '';
                        if (operator === 'exclude') activeFilters.account_website.excludes = '';
                    } else if (field === 'industry') {
                        if (operator === 'contains') activeFilters.account_industry.contains = '';
                        if (operator === 'starts_with') activeFilters.account_industry.starts_with = '';
                        if (operator === 'include') activeFilters.account_industry.includes = '';
                        if (operator === 'exclude') activeFilters.account_industry.excludes = '';
                    } else if (field === 'employee_count') {
                        if (operator === 'contains') activeFilters.account_employee_count_range.contains = '';
                        if (operator === 'starts_with') activeFilters.account_employee_count_range.starts_with = '';
                        if (operator === 'include') activeFilters.account_employee_count_range.includes = '';
                        if (operator === 'exclude') activeFilters.account_employee_count_range.excludes = '';
                    } else if (field === 'founded_year') {
                        activeFilters.account_founded_year = '';
                    } else if (field === 'country') {
                        // Remove the country from the array
                        const index = activeFilters.country.indexOf(value);
                        if (index > -1) {
                            activeFilters.country.splice(index, 1);
                        }
                        // Uncheck the corresponding checkbox
                        $(`#country_${value.replace(/\s+/g, '_')}`).prop('checked', false);
                    } else if (field === 'city') {
                        // Remove the city from the array
                        const index = activeFilters.city.indexOf(value);
                        if (index > -1) {
                            activeFilters.city.splice(index, 1);
                        }
                        // Uncheck the corresponding checkbox
                        $(`#city_${value.replace(/\s+/g, '_')}`).prop('checked', false);
                    }

                    $(this).closest('.filter-tag').remove();
                } else if (type && value) {
                    // Remove filter tag
                    $(this).closest('.filter-tag').remove();

                    if (type === 'filter') {
                        // Uncheck the corresponding checkbox
                        $(`#filter_${value}`).prop('checked', false);

                        // Remove from active filter buttons
                        const index = activeFilterButtons.indexOf(value);
                        if (index > -1) {
                            activeFilterButtons.splice(index, 1);
                            removeFilterButton(value);
                        }
                    }
                }

                // Reload data
                table.ajax.reload();
            });

            // Expandable Rows - Show all available data
            $('#tuesdayTable tbody').on('click', 'td:first-child', function () {
                var tr = $(this).closest('tr');
                var row = table.row(tr);
                var icon = $(this).find('.expand-icon');

                if (row.child.isShown()) {
                    // This row is already open - close it
                    row.child.hide();
                    tr.removeClass('shown');
                    icon.text('▸');
                } else {
                    // Open this row
                    var rowData = row.data();

                    // Build the expanded content HTML
                    var expandedContent = '<div class="expanded-row">';

                    // Loop through all properties in rowData and display them
                    for (var key in rowData) {
                        if (rowData.hasOwnProperty(key) && !fixedDisplayColumns.includes(key)) {
                            var value = rowData[key] || 'Not Found';
                            if (value && value.toLowerCase() !== 'nan' && value !== null) {
                                // Format the key for display
                                var displayKey = key
                                    .replace(/_/g, ' ')
                                    .replace(/\b\w/g, l => l.toUpperCase());

                                expandedContent += `
                                <div class="expanded-row-item">
                                    <strong>${displayKey}:</strong> <span>${value}</span>
                                </div>`;
                            }
                        }
                    }

                    expandedContent += '</div>';

                    row.child(expandedContent).show();
                    tr.addClass('shown');
                    icon.text('▾');
                }
            });

            // Select/Deselect All Columns
            $('#selectAllColumns').change(function () {
                const isChecked = $(this).prop('checked');
                $('input[name="export_column"]').prop('checked', isChecked);
            });

            // Check if all columns are selected and update the "Select All" checkbox
            $('input[name="export_column"]').change(function () {
                const allChecked = $('input[name="export_column"]').length === $('input[name="export_column"]:checked').length;
                $('#selectAllColumns').prop('checked', allChecked);
            });

            // Export popup
            $('#openExportPopup').click(function () {
                $('#exportPopupOverlay').show();
                $('#exportPopup').show();
            });

            $('#closeExportPopup, #cancelExport').click(function () {
                $('#exportPopupOverlay').hide();
                $('#exportPopup').hide();
            });

            // Close export popup when clicking on overlay
            $('#exportPopupOverlay').click(function () {
                $('#exportPopupOverlay').hide();
                $('#exportPopup').hide();
            });
            
            // Flush Cache button
            $('#flushCacheBtn').click(function() {
                $('#confirmationModal').css('display', 'flex');
            });
            
            // Cancel flush cache
            $('#cancelFlushCache').click(function() {
                $('#confirmationModal').css('display', 'none');
            });
            
            // Close confirmation modal when clicking outside
            $('#confirmationModal').click(function(e) {
                if ($(e.target).is('#confirmationModal')) {
                    $('#confirmationModal').css('display', 'none');
                }
            });

            // Export data with CSV button
            $('#exportCsvBtn').click(function () {
                const startRow = $('#exportStartRow').val() || 1;
                const endRow = $('#exportEndRow').val() || 100;
                const limit = $('#exportLimit').val() || '';

                // Get selected columns
                const selectedColumns = [];
                $('input[name="export_column"]:checked').each(function () {
                    selectedColumns.push($(this).val());
                });

                // Create URL with parameters for CSV export
                let exportUrl = 'data.php?action=exportToCSV';

                // Add row range parameters
                exportUrl += '&startRow=' + startRow;
                exportUrl += '&endRow=' + endRow;

                // Add limit if provided
                if (limit) {
                    exportUrl += '&limit=' + limit;
                }

                // Add columns parameter
                exportUrl += '&columns=' + selectedColumns.join(',');

                // Add all active filters as parameters
                if (activeFilters.searchValue) {
                    exportUrl += '&searchValue=' + encodeURIComponent(activeFilters.searchValue);
                }
                
                // Account Name filters
                if (activeFilters.account_name.contains) {
                    exportUrl += '&accountNameContains=' + encodeURIComponent(activeFilters.account_name.contains);
                }
                if (activeFilters.account_name.starts_with) {
                    exportUrl += '&accountNameStartsWith=' + encodeURIComponent(activeFilters.account_name.starts_with);
                }
                if (activeFilters.account_name.includes) {
                    exportUrl += '&accountNameIncludes=' + encodeURIComponent(activeFilters.account_name.includes);
                }
                if (activeFilters.account_name.excludes) {
                    exportUrl += '&accountNameExcludes=' + encodeURIComponent(activeFilters.account_name.excludes);
                }

                // Website filters
                if (activeFilters.account_website.contains) {
                    exportUrl += '&accountWebsiteContains=' + encodeURIComponent(activeFilters.account_website.contains);
                }
                if (activeFilters.account_website.starts_with) {
                    exportUrl += '&accountWebsiteStartsWith=' + encodeURIComponent(activeFilters.account_website.starts_with);
                }
                if (activeFilters.account_website.includes) {
                    exportUrl += '&accountWebsiteIncludes=' + encodeURIComponent(activeFilters.account_website.includes);
                }
                if (activeFilters.account_website.excludes) {
                    exportUrl += '&accountWebsiteExcludes=' + encodeURIComponent(activeFilters.account_website.excludes);
                }

                // Industry filters
                if (activeFilters.account_industry.contains) {
                    exportUrl += '&accountIndustryContains=' + encodeURIComponent(activeFilters.account_industry.contains);
                }
                if (activeFilters.account_industry.starts_with) {
                    exportUrl += '&accountIndustryStartsWith=' + encodeURIComponent(activeFilters.account_industry.starts_with);
                }
                if (activeFilters.account_industry.includes) {
                    exportUrl += '&accountIndustryIncludes=' + encodeURIComponent(activeFilters.account_industry.includes);
                }
                if (activeFilters.account_industry.excludes) {
                    exportUrl += '&accountIndustryExcludes=' + encodeURIComponent(activeFilters.account_industry.excludes);
                }

                // Employee Count filters
                if (activeFilters.account_employee_count_range.contains) {
                    exportUrl += '&accountEmployeeCountContains=' + encodeURIComponent(activeFilters.account_employee_count_range.contains);
                }
                if (activeFilters.account_employee_count_range.starts_with) {
                    exportUrl += '&accountEmployeeCountStartsWith=' + encodeURIComponent(activeFilters.account_employee_count_range.starts_with);
                }
                if (activeFilters.account_employee_count_range.includes) {
                    exportUrl += '&accountEmployeeCountIncludes=' + encodeURIComponent(activeFilters.account_employee_count_range.includes);
                }
                if (activeFilters.account_employee_count_range.excludes) {
                    exportUrl += '&accountEmployeeCountExcludes=' + encodeURIComponent(activeFilters.account_employee_count_range.excludes);
                }

                // Founded Year filter
                if (activeFilters.account_founded_year) {
                    exportUrl += '&accountFoundedYear=' + encodeURIComponent(activeFilters.account_founded_year);
                }

                // Country filter
                if (activeFilters.country.length > 0) {
                    exportUrl += '&country=' + encodeURIComponent(JSON.stringify(activeFilters.country));
                }

                // City filter
                if (activeFilters.city.length > 0) {
                    exportUrl += '&city=' + encodeURIComponent(JSON.stringify(activeFilters.city));
                }

                // Open the URL in a new tab/window to trigger the download
                window.open(exportUrl, '_blank');

                // Close popup
                $('#exportPopupOverlay').hide();
                $('#exportPopup').hide();
            });

            // Close modals when clicking outside
            $(window).click(function (event) {
                if (event.target.className === 'modal') {
                    $('.modal').css('display', 'none');
                }
            });
        });
    </script>
</body>

</html>
