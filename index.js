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

app.get('/home/spending',
    [
        validation.check('userId').not().isEmpty(),
    ], jsonParser,
    (req, res) => {
        if (!req.header('apiKey') || req.header('apiKey') !== API_KEY) {
            return res.status(401).json({status: 'error', message: 'Unauthorized.'})
        }

        const errors = validation.validationResult(req)

        if (!errors.isEmpty()) {
            return res.status(422).json({errors: errors.array()})
        }

        const {userId} = req.body;

        pool.query(`select u.id           as userId,
       sum(t.amt_rub) as totalAmt
from "user" u
         inner join account a on u.id = a.user_id
         inner join transaction t on t.from_id = a.id
where u.id = $1
and date_trunc('month', t.transaction_dttm) = date_trunc('month', current_date)
group by u.id
`, [userId], (error, results) => {
            if (error) {
                throw error
            }
            if (results.rows.length === 0)
                res.status(200).json({userId: userId, totalAmt: 0})
            else
                res.status(200).json(results.rows)
        })
    });

app.get('/limits',
    [
        validation.check('accountId').not().isEmpty(),
    ], jsonParser,
    async (req, res) => {
        if (!req.header('apiKey') || req.header('apiKey') !== API_KEY) {
            return res.status(401).json({status: 'error', message: 'Unauthorized.'})
        }

        const errors = validation.validationResult(req)

        if (!errors.isEmpty()) {
            return res.status(422).json({errors: errors.array()})
        }

        const {accountId} = req.body;

        let limits = await getCurrentUsageLimits(accountId);
        res.status(200).json({accountId: accountId, limits: limits})
    });

app.get('/home/accounts',
    [
        validation.check('userId').not().isEmpty(),
    ], jsonParser,
    (req, res) => {
        if (!req.header('apiKey') || req.header('apiKey') !== API_KEY) {
            return res.status(401).json({status: 'error', message: 'Unauthorized.'})
        }

        const errors = validation.validationResult(req)

        if (!errors.isEmpty()) {
            return res.status(422).json({errors: errors.array()})
        }

        const {userId} = req.body;

        pool.query(`select u.id as userId
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
and status_code <> 'CLO'
`, [userId], (error, results) => {
            if (error) {
                throw error
            }
            if (results.rows.length === 0)
                res.status(200).json({userId: userId, accounts: []})
            else
                res.status(200).json({userId: userId, accounts: results.rows})
        })
    });

app.post('/home/accounts',
    [
        validation.check('userId').not().isEmpty(),
    ], jsonParser,
    (req, res) => {
        if (!req.header('apiKey') || req.header('apiKey') !== API_KEY) {
            return res.status(401).json({status: 'error', message: 'Unauthorized.'})
        }

        const errors = validation.validationResult(req)

        if (!errors.isEmpty()) {
            return res.status(422).json({errors: errors.array()})
        }

        const {userId} = req.body;

        pool.query(`insert into account (user_id, agreement_code)
 VALUES ($1, upper('CU-AGR-'||(floor(random() * 10000000)::int)::text|| substring(md5(now()::varchar), 1, 8)))
`, [userId], (error, results) => {
            if (error) {
                return res.status(422).json({errors: errors.array()})
            } else {
                res.status(200).json({status: 'ok', message: 'Account created.'})
            }
        })
    });

app.get('/services', jsonParser,
    (req, res) => {
        if (!req.header('apiKey') || req.header('apiKey') !== API_KEY) {
            return res.status(401).json({status: 'error', message: 'Unauthorized.'})
        }

        pool.query(`select id
, service_nm as serviceName
, service_img_url as imgUrl
, merchant_nm as providerName
from merchant m
where service_flg = 1
`, (error, results) => {
            if (error) {
                throw error
            }
            res.status(200).json(results.rows)
        })
    });

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

async function updateUsageLimits(accountId, sumAmt) {
    try {
        const res = await pool.query(`update account_x_limit axl set rub_spent_amt = rub_spent_amt + $2::numeric 
where account_id = $1 and month_dt = date_trunc('month', current_date)`, [accountId, sumAmt]);
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

async function insertTransaction(fromType, fromId, toType, toId, type, transactionAmtFloat, comment) {
    try {
        const res = await pool.query(`insert into transaction ( from_type, from_id, to_type, to_id, transaction_type, amt_rub, comment) 
VALUES ($1, $2, $3, $4, $5, $6, $7)`, [fromType, fromId, toType, toId, type, transactionAmtFloat, comment]);
    } catch (err) {
        console.error(err.stack);
    }

}

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
            return res.status(401).json({status: 'error', message: 'Unauthorized.'})
        }

        const errors = validation.validationResult(req)

        if (!errors.isEmpty()) {
            return res.status(422).json({errors: errors.array()})
        }

        const {fromId, fromType, toId, toType, type, amtRub, comment} = req.body;
        let transactionAmtFloat = Number.parseFloat(amtRub);

        let isBalanceEnough = await balanceEnough(fromType, fromId, transactionAmtFloat);
        if (isBalanceEnough) {
            await insertTransaction(fromType, fromId, toType, toId, type, transactionAmtFloat, comment)
            if (fromType === 'account') {
                await updateBalance(fromId, -transactionAmtFloat);
            }
            if (toType === 'account') {
                await updateBalance(toId, transactionAmtFloat);

                let limits = await getCurrentUsageLimits(fromId);
                let limit = _.find(limits, function(o) { return o.merchantAccountId === toId; });
                if(limit){

                }
            }

            return res.status(200).json({status: 'ok', message: 'Transaction created.'});
        } else {
            return res.status(400).json({status: 'error', message: 'Not enough money.'})
        }

    });

// Start server
app.listen(process.env.PORT || 3002, () => {
    console.log(`Server listening`)
})