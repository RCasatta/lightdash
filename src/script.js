// Table sorting functionality
document.addEventListener('DOMContentLoaded', function () {
    const tables = document.querySelectorAll('table.sortable');

    tables.forEach(table => {
        const headers = table.querySelectorAll('th');
        const tbody = table.querySelector('tbody');

        headers.forEach((header, index) => {
            // Make header clickable
            header.style.cursor = 'pointer';
            header.style.userSelect = 'none';

            // Add sorting indicator
            const indicator = document.createElement('span');
            indicator.className = 'sort-indicator';
            indicator.textContent = ' ↕';
            indicator.style.opacity = '0.3';
            header.appendChild(indicator);

            let sortOrder = 'asc';

            header.addEventListener('click', function () {
                // Remove indicators from other headers
                headers.forEach(h => {
                    const ind = h.querySelector('.sort-indicator');
                    if (ind && h !== header) {
                        ind.textContent = ' ↕';
                        ind.style.opacity = '0.3';
                    }
                });

                // Get all rows
                const rows = Array.from(tbody.querySelectorAll('tr'));

                // Determine sort direction
                sortOrder = sortOrder === 'asc' ? 'desc' : 'asc';

                // Update indicator
                indicator.textContent = sortOrder === 'asc' ? ' ▲' : ' ▼';
                indicator.style.opacity = '1';

                // Sort rows
                rows.sort((a, b) => {
                    const aCell = a.querySelectorAll('td')[index];
                    const bCell = b.querySelectorAll('td')[index];

                    if (!aCell || !bCell) return 0;

                    let aValue = aCell.textContent.trim();
                    let bValue = bCell.textContent.trim();

                    // Handle N/A and - values
                    if (aValue === 'N/A' || aValue === '-') aValue = sortOrder === 'asc' ? 'zzz' : '';
                    if (bValue === 'N/A' || bValue === '-') bValue = sortOrder === 'asc' ? 'zzz' : '';

                    // Check if values are dates (format: YYYY-MM-DD HH:MM:SS or similar)
                    const datePattern = /^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}:\d{2})?$/;
                    if (datePattern.test(aValue) && datePattern.test(bValue)) {
                        const aDate = new Date(aValue);
                        const bDate = new Date(bValue);
                        return sortOrder === 'asc' ? aDate - bDate : bDate - aDate;
                    }

                    // Try to parse as number (including percentages)
                    const aNum = parseFloat(aValue.replace(/[,%]/g, ''));
                    const bNum = parseFloat(bValue.replace(/[,%]/g, ''));

                    if (!isNaN(aNum) && !isNaN(bNum)) {
                        // Numeric comparison
                        return sortOrder === 'asc' ? aNum - bNum : bNum - aNum;
                    } else {
                        // String comparison
                        if (sortOrder === 'asc') {
                            return aValue.localeCompare(bValue);
                        } else {
                            return bValue.localeCompare(aValue);
                        }
                    }
                });

                // Reorder rows in the DOM
                rows.forEach(row => tbody.appendChild(row));
            });
        });
    });
});

