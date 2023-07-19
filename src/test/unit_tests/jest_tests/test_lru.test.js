/* Copyright (C) 2016 NooBaa */
/* eslint-disable no-undef */
'use strict';

const LRU = require('../../../util/lru');
const P = require('../../../util/promise');

describe('lru', () => {
    it('should hit and miss after remove', () => {
        const lru = new LRU();
        lru._sanity();

        let item = lru.find_or_add_item(1);
        item.foo = 'bar';
        lru._sanity();

        item = lru.find_or_add_item(1);
        expect(item.foo).toBe('bar');
        lru._sanity();

        lru.remove_item(1);
        lru._sanity();

        item = lru.find_or_add_item(1);
        expect(item.foo).toBeUndefined();
        lru._sanity();
    });

    it('should remove item to make room', () => {
        const lru = new LRU({
            max_usage: 1
        });
        lru._sanity();

        let item = lru.find_or_add_item(1);
        item.foo = 'bar';
        lru._sanity();

        item = lru.find_or_add_item(2);
        lru._sanity();

        item = lru.find_or_add_item(1);
        expect(item.foo).toBeUndefined();
        lru._sanity();
    });

    it('should remove expired item', async () => {
        const lru = new LRU({
            expiry_ms: 100
        });
        lru._sanity();

        let item = lru.find_or_add_item(1);
        item.foo = 'bar';
        lru._sanity();

        await P.delay(1);

        item = lru.find_or_add_item(1);
        expect(item.foo).toBe('bar');
        lru._sanity();

        await P.delay(110);

        lru._sanity();
        item = lru.find_or_add_item(1);
        expect(item.foo).toBeUndefined();
        lru._sanity();
    });

    it('should return null for missing id', () => {
        const lru = new LRU();
        lru._sanity();

        lru.find_or_add_item(1);
        expect(lru.remove_item(1)).toBeTruthy();
        expect(lru.remove_item(1)).toBeFalsy();
        lru._sanity();
    });

    it('should handle max_usage = 0', () => {
        const lru = new LRU({
            max_usage: 0,
        });
        lru._sanity();

        const item = lru.find_or_add_item(1);
        expect(item).toBeTruthy();
        expect(lru.usage).toBe(0);
        expect(item.usage).toBe(1);
        lru._sanity();

        lru.set_usage(item, 3);
        expect(item.usage).toBe(3);
        expect(lru.usage).toBe(0);
        lru._sanity();

        const item1 = lru.find_or_add_item(1);
        expect(item1).not.toBe(item);
        lru._sanity();
    });

    it('should respect max_usage', () => {
        const MAX_USAGE = 1000;
        const lru = new LRU({
            max_usage: MAX_USAGE,
        });
        lru._sanity();

        for (let i = 0; i < 1000; ++i) {
            const key = Math.floor(100 * Math.random());
            const item = lru.find_or_add_item(key);
            lru.set_usage(item, Math.floor(MAX_USAGE * Math.random()));
            lru._sanity();
        }
    });
});
