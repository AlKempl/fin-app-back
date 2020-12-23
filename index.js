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

        const userId = req.query.userId;

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

        const accountId = req.query.accountId;

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

        const userId = req.query.userId;

        let userData = await userDetails(userId);
        res.status(200).json(userData)
    });

app.get('/auth', jsonParser,
    async (req, res) => {
        if (!req.header('apiKey') || req.header('apiKey') !== API_KEY) {
            return res.status(401).json({status: 'error', code: 'TA', message: 'Unauthorized.'})
        }

        let userId = await addUser();
        let mainAccountId = await addBasicAccounts(userId);
        await addBasicLimits(mainAccountId);

        return res.status(200).json({userid: userId, mainaccountid: mainAccountId});
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

        const userId = req.query.userId;

        let accounts = await userAccounts(userId);
        res.status(200).json({userId: userId, accounts: accounts});
    });

app.get('/account/transactions',
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

        const accountId = req.query.accountId;

        let isUsable = await getAccountStatus(accountId);
        if (!isUsable) {
            return res.status(400).json({status: 'error', code: 'A1', message: 'Account is not usable.'});
        }

        let transactionsList = await accountTransactions(accountId);
        return res.status(200).json({accountId: accountId, transactions: transactionsList});
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

app.get('/statement',
    [
        validation.check('userId').not().isEmpty(),
    ], jsonParser,
    async (req, res) => {
        if (!req.header('apiKey') || req.header('apiKey') !== API_KEY) {
            return res.status(401).json({status: 'error', code: 'TA', message: 'Unauthorized.'})
        }

        const userId = req.query.userId;

        let mainAccountId = await getMainAccount(userId);
        let protectionAccountId = await getProtectionAccount(mainAccountId);
        let fund = {accountId: 12, id: 7, name: '"Благо.ру"'}

        let limits = await getCurrentUsageLimits(mainAccountId);
        for (let limit in limits) {
            let rubSpentAmt = Number.parseFloat(limit.rubspentamt);
            let rubLimitAmt = Number.parseFloat(limit.rublimitamt);
            let merchantId = Number.parseInt(limit.merchantid);
            let savedAmt = rubSpentAmt - rubLimitAmt;
            if (rubSpentAmt > rubLimitAmt * 2) {
                // transfer money to a fund
                await insertTransaction('account', protectionAccountId, 'account', fund.accountId, 'transfer', savedAmt, "Перевод защищенных средств в фонд " + fund.name)
                await updateBalance(protectionAccountId, -savedAmt);
                await updateBalance(fund.accountId, savedAmt);
            } else {
                // chargeback
                await insertTransaction('account', protectionAccountId, 'account', mainAccountId, 'chargeback', savedAmt, "Возврат защищенных средств")
                await updateBalance(protectionAccountId, -savedAmt);
                await updateBalance(mainAccountId, savedAmt);
            }
            await resetUsageLimits(mainAccountId, merchantId);
        }

        let feeAmt = 20.0;
        await insertTransaction('account', mainAccountId, 'external', 4242, 'fee', feeAmt, "Комиссия за использование копилки")
        await updateBalance(mainAccountId, -feeAmt);

        // increment statement_dt with a month

        res.status(200).json({status: 'ok', statement: 'done'});
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
, a.emitter
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

async function accountTransactions(accountId) {
    try {
        const res = await pool.query(`select id as transactionId
     , from_type as fromType
     , from_id as fromId
     , to_type as toType
     , to_id as toId
     , transaction_type as transactionType
     , transaction_dttm as transactionTimestamp
     , amt_rub as transactionAmtRub
     , comment
     , transfer_to_merchant_id as shouldBeTransferedToMerchantId
     , (t.to_type = 'account' and t.to_id = $1) as isIncoming
from transaction t
where (t.from_type = 'account' and t.from_id = $1)
   or (t.to_type = 'account' and t.to_id = $1)`, [accountId]);
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
, service_icon as icon
, merchant_nm as providerName
, default_purchase_amt as defaultSumAmt
, default_purchase_comment as defaultComment
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


async function addUser() {
    let userId = 1;
    try {
        const res = await pool.query(`INSERT INTO "user" (id, first_nm, last_nm, middle_nm, login, pass_hash, created_dttm, statement_dt, auth_key)
VALUES (DEFAULT, 'Сидоров', 'Иван', 'Петрович', random_string(10), random_string(15), DEFAULT, DEFAULT, DEFAULT) returning id`, []);
        userId = res.rows[0]['id'];
    } catch (err) {
        console.error(err.stack);
    }
    return userId;
}


async function addBasicAccounts(userId) {
    let mainAccountId = 4;
    try {
        const res = await pool.query(`INSERT INTO account (user_id, type_code, caption, open_dttm, close_dttm, transfer_to_merchant_id,
                            agreement_code, balance_rub_amt, merchant_id, status_code, emitter)
VALUES ($1, 'main', 'Счет', current_timestamp, null, null, 
upper('CU-AGR-'||(floor(random() * 1000000)::int)::text|| substring(md5(now()::varchar), 1, 8) || '-1') , 29882.6800, null, 'ACT',
        'tnkf') returning id`, [userId]);
        mainAccountId = res.rows[0]['id'];

       const res2 = await pool.query(`INSERT INTO account (user_id, type_code, caption, open_dttm, close_dttm, transfer_to_merchant_id,
                            agreement_code, balance_rub_amt, merchant_id, status_code, emitter)
VALUES ($1, 'protected', 'Защитная копилка', current_timestamp, null, null,
 upper('CU-AGR-'||(floor(random() * 1000000)::int)::text|| substring(md5(now()::varchar), 1, 8) || '-2'), 0.0000,
        null, 'ACT', 'unknown')`, [userId]);

        return mainAccountId;
    } catch (err) {
        console.error(err.stack);
    }
}

async function addBasicLimits(mainAccountId) {
    try {
        await pool.query(`INSERT INTO public.account_x_limit (account_id, merchant_id, rub_limit_amt, month_dt, rub_spent_amt)
VALUES ($1, 1, 1200.0000, '2020-12-01', 0.0000)`, [mainAccountId]);
        await pool.query(`INSERT INTO public.account_x_limit (account_id, merchant_id, rub_limit_amt, month_dt, rub_spent_amt)
VALUES ($1, 2, 1200.0000, '2020-12-01', 0.0000)`, [mainAccountId]);
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

async function resetUsageLimits(accountId, merchantId) {
    try {
        const res = await pool.query(`update account_x_limit axl set rub_spent_amt = 0.0 
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

async function getMainAccount(userId) {
    try {
        const res = await pool.query(`select id as mainAccountId
from account a
where user_id = $1
and a.type_code = 'main'
and a.status_code = 'ACT'
and now() between a.open_dttm and coalesce(a.close_dttm, '5999-01-01'::timestamp)
limit 1`, [accountId]);
        if (res.rows.length === 0)
            return null
        else
            return res.rows[0]['mainaccountid'];
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