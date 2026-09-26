(async () => {

    console.log("========================================");
    console.log("TIKTOK METADATA CRAWLER");
    console.log("========================================");

    const keyword = window.SEARCH_KEYWORD || "";
    const province = window.PROVINCE || "";
    const category = window.CATEGORY || "";
    const quesId = window.QUES_ID || "";

    const crawlTime = new Date().toISOString();

    const results = new Map();

    const MAX_SCROLL = 100;
    const WAIT_TIME = 2500;
    const MAX_IDLE = 5;

    function sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    // --------------------------------------------------------
    // CLEAN TEXT
    // --------------------------------------------------------

    function cleanText(text) {

        if (!text) {
            return "";
        }

        return text
            .replace(/\s+/g, " ")
            .trim();
    }

    // --------------------------------------------------------
    // GET TEXT
    // --------------------------------------------------------

    function getText(element) {

        if (!element) {
            return "";
        }

        return cleanText(
            element.innerText ||
            element.textContent ||
            ""
        );
    }

    // --------------------------------------------------------
    // EXTRACT NUMBER
    // --------------------------------------------------------

    function parseCount(text) {

        if (!text) {
            return null;
        }

        text = text
            .trim()
            .replace(/\s/g, "")
            .toUpperCase();

        // 1.2K
        if (text.endsWith("K")) {

            const value =
                parseFloat(
                    text.replace("K", "")
                );

            if (!isNaN(value)) {
                return Math.round(value * 1000);
            }
        }

        // 1.2M
        if (text.endsWith("M")) {

            const value =
                parseFloat(
                    text.replace("M", "")
                );

            if (!isNaN(value)) {
                return Math.round(value * 1000000);
            }
        }

        // 1.2B
        if (text.endsWith("B")) {

            const value =
                parseFloat(
                    text.replace("B", "")
                );

            if (!isNaN(value)) {
                return Math.round(value * 1000000000);
            }
        }

        // 1,234
        const number =
            parseInt(
                text.replace(/[^\d]/g, ""),
                10
            );

        if (!isNaN(number)) {
            return number;
        }

        return null;
    }

    // --------------------------------------------------------
    // HASHTAGS
    // --------------------------------------------------------

    function extractHashtags(text) {

        if (!text) {
            return "";
        }

        const matches =
            text.match(
                /#[\p{L}\p{N}_]+/gu
            );

        if (!matches) {
            return "";
        }

        return [
            ...new Set(matches)
        ].join("|");
    }

    // --------------------------------------------------------
    // FIND VIDEO CARDS
    // --------------------------------------------------------

    function getCards() {

        const selectors = [

            '[data-e2e="search_top-item-list"]',

            '[data-e2e="search-card-video"]',

            '[data-e2e="search_video-item"]',

            'div[class*="DivItemContainer"]',

            'div[class*="DivItemContainerV2"]',

            'div[class*="DivSearchCard"]'

        ];

        const cards = [];

        selectors.forEach(selector => {

            document
                .querySelectorAll(selector)
                .forEach(element => {

                    if (!cards.includes(element)) {
                        cards.push(element);
                    }

                });

        });

        return cards;
    }

    // --------------------------------------------------------
    // EXTRACT CARD
    // --------------------------------------------------------

    function extractCard(card) {

        if (!card) {
            return null;
        }

        const text =
            getText(card);

        if (!text) {
            return null;
        }

        // --------------------------------------------
        // AUTHOR
        // --------------------------------------------

        let author = "";

        const authorElement =
            card.querySelector(
                '[data-e2e="search-card-user-link"]'
            ) ||
            card.querySelector(
                '[data-e2e="search-card-user-unique-id"]'
            ) ||
            card.querySelector(
                'a[href*="/@"]'
            );

        if (authorElement) {

            author =
                getText(authorElement);

            if (
                authorElement.href &&
                authorElement.href.includes("/@")
            ) {

                const match =
                    authorElement.href.match(
                        /\/@([^/?]+)/ 
                    );

                if (match) {
                    author = match[1];
                }
            }
        }

        // --------------------------------------------
        // DESCRIPTION
        // --------------------------------------------

        let description = "";

        const descriptionElement =
            card.querySelector(
                '[data-e2e="search-card-desc"]'
            ) ||
            card.querySelector(
                '[data-e2e="search-card-caption"]'
            );

        if (descriptionElement) {

            description =
                getText(descriptionElement);

        }

        if (!description) {

            description = text;

        }

        // --------------------------------------------
        // HASHTAGS
        // --------------------------------------------

        const hashtags =
            extractHashtags(
                description
            );

        // --------------------------------------------
        // POSTED DATE
        // --------------------------------------------

        let postedDate = "";

        const dateElement =
            card.querySelector(
                '[data-e2e="search-card-video-caption"]'
            ) ||
            card.querySelector(
                'time'
            );

        if (dateElement) {

            postedDate =
                getText(dateElement);

        }

        // --------------------------------------------
        // STATS
        // --------------------------------------------

        let viewCount = null;
        let likeCount = null;
        let commentCount = null;
        let shareCount = null;

        const numberElements =
            card.querySelectorAll(
                'strong, span'
            );

        const numbers = [];

        numberElements.forEach(element => {

            const value =
                getText(element);

            if (!value) {
                return;
            }

            if (
                /^[\d,.]+[KMB]?$/i.test(value)
            ) {

                numbers.push(
                    parseCount(value)
                );
            }

        });

        /*
         * TikTok search cards có thể không hiển thị
         * đầy đủ engagement.
         *
         * Không đoán dữ liệu nếu DOM không có.
         */

        if (numbers.length > 0) {
            viewCount = numbers[0] || null;
        }

        // --------------------------------------------
        // GENERATE RECORD ID
        // --------------------------------------------

        const recordText =
            author +
            "|" +
            description +
            "|" +
            postedDate;

        const recordId =
            btoa(
                unescape(
                    encodeURIComponent(
                        recordText
                    )
                )
            ).substring(0, 32);

        return {

            record_id: recordId,

            keyword: keyword,

            province: province,

            category: category,

            author: author,

            description: description,

            hashtags: hashtags,

            view_count: viewCount,

            like_count: likeCount,

            comment_count: commentCount,

            share_count: shareCount,

            posted_date: postedDate,

            crawl_time: crawlTime,

            ques_id: quesId

        };
    }

    // --------------------------------------------------------
    // COLLECT
    // --------------------------------------------------------

    function collectMetadata() {

        const cards =
            getCards();

        console.log(
            "Cards found:",
            cards.length
        );

        cards.forEach(card => {

            const record =
                extractCard(card);

            if (!record) {
                return;
            }

            const key =
                record.record_id;

            if (!results.has(key)) {

                results.set(
                    key,
                    record
                );
            }

        });

        console.log(
            "Metadata collected:",
            results.size
        );
    }

    // --------------------------------------------------------
    // SCROLL
    // --------------------------------------------------------

    let lastCount = 0;
    let idleRounds = 0;

    for (
        let i = 0;
        i < MAX_SCROLL;
        i++
    ) {

        collectMetadata();

        window.scrollTo(
            0,
            document.body.scrollHeight
        );

        await sleep(
            WAIT_TIME
        );

        if (
            results.size === lastCount
        ) {

            idleRounds++;

        } else {

            idleRounds = 0;

        }

        lastCount =
            results.size;

        console.log(
            `Scroll ${i + 1}/${MAX_SCROLL} | ` +
            `records=${results.size}`
        );

        if (
            idleRounds >= MAX_IDLE
        ) {

            console.log(
                "No new metadata found."
            );

            break;
        }
    }

    // Collect one final time
    collectMetadata();

    // --------------------------------------------------------
    // EXPORT CSV
    // --------------------------------------------------------

    const rows =
        Array.from(
            results.values()
        );

    console.log(
        "========================================"
    );

    console.log(
        "TOTAL METADATA:",
        rows.length
    );

    console.log(
        "========================================"
    );

    if (rows.length === 0) {

        console.warn(
            "No metadata found."
        );

        return;
    }

    const headers = [

        "record_id",
        "keyword",
        "province",
        "category",
        "author",
        "description",
        "hashtags",
        "view_count",
        "like_count",
        "comment_count",
        "share_count",
        "posted_date",
        "crawl_time",
        "ques_id"

    ];

    function escapeCSV(value) {

        if (
            value === null ||
            value === undefined
        ) {

            return "";

        }

        value =
            String(value);

        if (
            value.includes('"') ||
            value.includes(",") ||
            value.includes("\n")
        ) {

            return (
                '"' +
                value.replace(
                    /"/g,
                    '""'
                ) +
                '"'
            );

        }

        return value;
    }

    let csv =
        headers.join(",") +
        "\n";

    rows.forEach(row => {

        csv +=
            headers
                .map(
                    header =>
                        escapeCSV(
                            row[header]
                        )
                )
                .join(",") +
            "\n";

    });

    const blob =
        new Blob(
            [
                "\ufeff" + csv
            ],
            {
                type:
                    "text/csv;charset=utf-8;"
            }
        );

    const downloadUrl =
        URL.createObjectURL(
            blob
        );

    const link =
        document.createElement("a");

    link.href =
        downloadUrl;

    link.download =
        "tiktok_metadata.csv";

    document.body.appendChild(link);

    link.click();

    link.remove();

    URL.revokeObjectURL(
        downloadUrl
    );

    console.log(
        "Metadata CSV downloaded."
    );

})();