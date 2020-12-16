const validation = require("express-validator");
const express = require('express')
const bodyParser = require('body-parser')
const helmet = require('helmet')
const compression = require('compression')
const cors = require('cors')
const {API_KEY} = require("./config");
const {pool} = require('./config')
const _ = require('lodash');

const app = express()

app.use(bodyParser.json())
app.use(bodyParser.urlencoded({extended: true}))
app.use(cors())
app.use(helmet())
app.use(compression())

var jsonParser = bodyParser.json()

app.get('/',
    async (req, res) => {
        return res.status(400).json({status: 'error', code: 'TR', message: 'No api method requested.'})
    });

app.get('/home/spending',
    [
        validation.check('userId').not().isEmpty(),
    ], jsonParser,
    async (req, res) => {
        if (!req.header('apiKey') || req.header('apiKey') !== API_KEY) {
            return res.status(401).json({status: 'error', code: 'TA', message: 'Unauthorized.'})
        }

        const errors = validation.validationResult(req)

        if (!errors.isEmpty()) {
            return res.status(422).json({status: 'error', code: 'TN', errors: errors.array()})
        }

        const {userId} = req.body;

        let userSpendingData = await userSpending(userId);
        res.status(200).json({userId: userId, totalAmt: userSpendingData});
    });

app.get('/limits',
    [
        validation.check('accountId').not().isEmpty(),
    ], jsonParser,
    async (req, res) => {
        if (!req.header('apiKey') || req.header('apiKey') !== API_KEY) {
            return res.status(401).json({status: 'error', code: 'TA', message: 'Unauthorized.'})
        }

        const errors = validation.validationResult(req)

        if (!errors.isEmpty()) {
            return res.status(422).json({status: 'error', code: 'TN', errors: errors.array()})
        }

        const {accountId} = req.body;

        let limits = await getCurrentUsageLimits(accountId);
        res.status(200).json({accountId: accountId, limits: limits})
    });

app.get('/home/user',
    [
        validation.check('userId').not().isEmpty(),
    ], jsonParser,
    async (req, res) => {
        if (!req.header('apiKey') || req.header('apiKey') !== API_KEY) {
            return res.status(401).json({status: 'error', code: 'TA', message: 'Unauthorized.'})
        }

        const errors = validation.validationResult(req)

        if (!errors.isEmpty()) {
            return res.status(422).json({status: 'error', code: 'TN', errors: errors.array()})
        }

        const {userId} = req.body;

        let userData = await userDetails(userId);
        res.status(200).json(userData)
    });

app.get('/auth',
    [
        validation.check('authKey').not().isEmpty(),
    ], jsonParser,
    async (req, res) => {
        if (!req.header('apiKey') || req.header('apiKey') !== API_KEY) {
            return res.status(401).json({status: 'error', code: 'TA', message: 'Unauthorized.'})
        }

        const errors = validation.validationResult(req)

        if (!errors.isEmpty()) {
            return res.status(422).json({status: 'error', code: 'TN', errors: errors.array()})
        }

        const {authKey} = req.body;

        let userData = await userAuthWithKey(authKey);

        if (!userData)
            return res.status(401).json({status: 'error', code: 'TK', message: 'Unauthorized.'})
        else
            return res.status(200).json(userData)
    });

app.get('/home/accounts',
    [
        validation.check('userId').not().isEmpty(),
    ], jsonParser,
    async (req, res) => {
        if (!req.header('apiKey') || req.header('apiKey') !== API_KEY) {
            return res.status(401).json({status: 'error', code: 'TA', message: 'Unauthorized.'})
        }

        const errors = validation.validationResult(req)

        if (!errors.isEmpty()) {
            return res.status(422).json({status: 'error', code: 'TN', errors: errors.array()})
        }

        const {userId} = req.body;

        let accounts = await userAccounts(userId);
        res.status(200).json({userId: userId, accounts: accounts});
    });

app.post('/home/accounts',
    [
        validation.check('userId').not().isEmpty(),
    ], jsonParser,
    async (req, res) => {
        if (!req.header('apiKey') || req.header('apiKey') !== API_KEY) {
            return res.status(401).json({status: 'error', code: 'TA', message: 'Unauthorized.'})
        }

        const errors = validation.validationResult(req)

        if (!errors.isEmpty()) {
            return res.status(422).json({status: 'error', code: 'TN', errors: errors.array()})
        }

        const {userId} = req.body;
        await addUserAccount(userId);
        res.status(200).json({status: 'ok', message: 'Account created.'})
    });

app.get('/services', jsonParser,
    async (req, res) => {
        if (!req.header('apiKey') || req.header('apiKey') !== API_KEY) {
            return res.status(401).json({status: 'error', code: 'TA', message: 'Unauthorized.'})
        }

        let services = await servicesList();
        res.status(200).json({status: 'ok', services: services});
    });

app.get('/statement', jsonParser,
    async (req, res) => {
        if (!req.header('apiKey') || req.header('apiKey') !== API_KEY) {
            return res.status(401).json({status: 'error', code: 'TA', message: 'Unauthorized.'})
        }

        let statement = await statementGet();
        res.status(200).json({status: 'ok', statement: statement});
    });

app.post('/transaction',
    [
        validation.check('fromId').not().isEmpty(),
        validation.check('fromType').not().isEmpty().isIn(['external', 'account']),
        validation.check('toId').not().isEmpty(),
        validation.check('toType').not().isEmpty().isIn(['external', 'account']),
        validation.check('type').not().isEmpty().isIn(['transfer', 'fee', 'purchase']),
        validation.check('amtRub').not().isEmpty(),
    ], jsonParser,
    async (req, res) => {
        if (!req.header('apiKey') || req.header('apiKey') !== API_KEY) {
            return res.status(401).json({status: 'error', code: 'TA', message: 'Unauthorized.'})
        }

        const errors = validation.validationResult(req)

        if (!errors.isEmpty()) {
            return res.status(422).json({status: 'error', code: 'TN', errors: errors.array()})
        }

        const {fromId, fromType, toId, toType, type, amtRub, comment} = req.body;
        let transactionAmtFloat = Number.parseFloat(amtRub);
        if (fromType === 'account') {
            let isUsable = await getAccountStatus(fromId);
            if (!isUsable) {
                return res.status(400).json({status: 'error', code: 'A1', message: 'Account is not usable.'});
            }
        }

        let isBalanceEnough = await balanceEnough(fromType, fromId, transactionAmtFloat);
        if (!isBalanceEnough) {
            return res.status(400).json({status: 'error', code: 'F1', message: 'Not enough money.'});
        }

        await insertTransaction(fromType, fromId, toType, toId, type, transactionAmtFloat, comment)

        if (fromType === 'account') {
            await updateBalance(fromId, -transactionAmtFloat);
        }
        if (toType === 'account') {
            await updateBalance(toId, transactionAmtFloat);
        }

        let limits = await getCurrentUsageLimits(fromId);
        let limit = _.find(limits, function (o) {
            let merchantAccountId = Number.parseInt(o.merchantaccountid);
            return merchantAccountId === Number.parseInt(toId);
        });

        let limit_exists = false;
        let protected_transaction = false;

        if (limit) {
            limit_exists = true;
            let merchantId = Number.parseInt(limit.merchantid);
            let newSpentAmt = await updateUsageLimits(fromId, merchantId, transactionAmtFloat);
            let rubSpentAmt = Number.parseFloat(newSpentAmt);
            let rubLimitAmt = Number.parseFloat(limit.rublimitamt);

            if (rubSpentAmt > rubLimitAmt) {
                protected_transaction = true;

                let protectionAccountId = await getProtectionAccount(fromId);

                await insertTransaction('account', fromId, 'account', protectionAccountId, 'protection', transactionAmtFloat, comment)
                await updateBalance(fromId, -transactionAmtFloat);
                await updateBalance(protectionAccountId, transactionAmtFloat);
            }
        }

        return res.status(200).json({
            status: 'ok',
            message: 'Transaction created.',
            limitExists: limit_exists,
            protectedTransaction: protected_transaction
        });


    });

async function userSpending(userId) {
    try {
        const res = await pool.query(`select sum(t.amt_rub) as totalAmt
from "user" u
         inner join account a on u.id = a.user_id
         inner join transaction t on t.from_id = a.id
where u.id = $1
and date_trunc('month', t.transaction_dttm) = date_trunc('month', current_date)
group by u.id`, [userId]);
        if (res.rows.length === 0)
            return 0;
        else
            return Number.parseFloat(res.rows[0]['totalamt']);
    } catch (err) {
        console.error(err.stack);
    }
}

async function userAccounts(userId) {
    try {
        const res = await pool.query(`select u.id as userId
, a.id as accountId
, a.type_code as accountType
, a.status_code as accountStatus
, a.caption
, a.transfer_to_merchant_id as transferToMerchantId
, a.agreement_code as agreementCode
, a.balance_rub_amt as balanceRubAmt
from "user" u
inner join account a on u.id = a.user_id
where u.id = $1
and now() between a.open_dttm and coalesce(a.close_dttm, '5999-01-01'::date)
and status_code <> 'CLO'`, [userId]);
        if (res.rows.length === 0)
            return [];
        else
            return res.rows;
    } catch (err) {
        console.error(err.stack);
    }
}

async function userDetails(userId) {
    try {
        const res = await pool.query(`select id as userId, first_nm as firstName, last_nm as lastName, 
                                        middle_nm as middleName, login, statement_dt as statementDate
                                        from "user" u
                                        where u.id = $1`, [userId]);
        if (res.rows.length === 0)
            return {};
        else
            return res.rows[0];
    } catch (err) {
        console.error(err.stack);
    }
}

async function userAuthWithKey(authKey) {
    try {
        const res = await pool.query(`select id as userId
                                        from "user" u
                                        where u.auth_key = $1
                                        limit 1`, [authKey]);
        if (res.rows.length === 0)
            return {};
        else
            return res.rows[0];
    } catch (err) {
        console.error(err.stack);
    }
}

async function servicesList() {
    try {
        const res = await pool.query(`select id
, service_nm as serviceName
, service_img_url as imgUrl
, merchant_nm as providerName
from merchant m
where service_flg = 1`, []);
        if (res.rows.length === 0)
            return [];
        else
            return res.rows;
    } catch (err) {
        console.error(err.stack);
    }
}

async function addUserAccount(userId) {
    try {
        const res = await pool.query(`insert into account (user_id, agreement_code, type_code)
 VALUES ($1, upper('CU-AGR-'||(floor(random() * 10000000)::int)::text|| substring(md5(now()::varchar), 1, 8)), 'additional')`, [userId]);
    } catch (err) {
        console.error(err.stack);
    }
}


async function balanceEnough(fromType, fromId, transactionAmtFloat) {
    if (fromType === 'external') {
        return true;
    } else {
        let balance = 0;
        try {
            const res = await pool.query(`select a.balance_rub_amt from account a where a.id = $1`, [fromId]);
            balance = res.rows[0]['balance_rub_amt'];
        } catch (err) {
            console.error(err.stack);
        }
        return balance >= transactionAmtFloat;
    }
}

async function updateBalance(fromId, transactionAmtFloat) {
    try {
        const res = await pool.query(`update account a set balance_rub_amt = balance_rub_amt + $2::numeric where a.id = $1`, [fromId, transactionAmtFloat]);
    } catch (err) {
        console.error(err.stack);
    }
}

async function updateUsageLimits(accountId, merchantId, sumAmt) {
    try {
        const res = await pool.query(`update account_x_limit axl set rub_spent_amt = rub_spent_amt + $3::numeric 
where account_id = $1 and merchant_id = $2 and month_dt = date_trunc('month', current_date) returning rub_spent_amt`, [accountId, merchantId, sumAmt]);
        if (res.rows.length === 0)
            return null
        else
            return res.rows[0]['rub_spent_amt'];
    } catch (err) {
        console.error(err.stack);
    }
}

async function getCurrentUsageLimits(accountId) {
    try {
        const res = await pool.query(`select axl.merchant_id as merchantId, axl.rub_limit_amt as rubLimitAmt,
       axl.month_dt as monthDt, axl.rub_spent_amt as rubSpentAmt, a.id as merchantAccountid
from account_x_limit axl
left join account a on axl.merchant_id = a.merchant_id
where axl.account_id = $1
and axl.month_dt = date_trunc('month', current_date)`, [accountId]);
        return res.rows;
    } catch (err) {
        console.error(err.stack);
    }
}

async function getProtectionAccount(accountId) {
    try {
        const res = await pool.query(`select id as protectionAccountId
from account a
where user_id in (select user_id from account a where a.id = $1 limit 1)
and a.type_code = 'protected'
and a.status_code = 'ACT'
and now() between a.open_dttm and coalesce(a.close_dttm, '5999-01-01'::timestamp)
limit 1`, [accountId]);
        if (res.rows.length === 0)
            return null
        else
            return res.rows[0]['protectionaccountid'];
    } catch (err) {
        console.error(err.stack);
    }
}

async function getAccountStatus(accountId) {
    try {
        const res = await pool.query(`select ((a.status_code = 'ACT')
        and (now() between a.open_dttm and coalesce(a.close_dttm, '5999-01-01'::timestamp)))::int as is_usable
        from account a
        where a.id = $1`, [accountId]);
        if (res.rows.length === 0)
            return false
        else
            return res.rows[0]['is_usable'];
    } catch (err) {
        console.error(err.stack);
    }
}

async function insertTransaction(fromType, fromId, toType, toId, type, transactionAmtFloat, comment) {
    try {
        const res = await pool.query(`insert into transaction ( from_type, from_id, to_type, to_id, transaction_type, amt_rub, comment) 
VALUES ($1, $2, $3, $4, $5, $6, $7)`, [fromType, fromId, toType, toId, type, transactionAmtFloat, comment]);
    } catch (err) {
        console.error(err.stack);
    }

}

// Start server
app.listen(process.env.PORT || 3002, () => {
    console.log(`Server listening`)
})