/* Copyright (C) 2016 NooBaa */
'use strict';

const { ObjectId } = require('./objectId_utils');

describe('ObjectId Utils', () => {

    it('should construct a new ObjectId', () => {
        const id = new ObjectId();
        expect(id).toBeDefined();
        expect(ObjectId.isValid(id)).toBe(true);
        expect(id.toHexString().length).toBe(24);
    });

    it('should construct from hex string', () => {
        const hex = '507f1f77bcf86cd799439011';
        const id = new ObjectId(hex);
        expect(id.toHexString()).toBe(hex);
        expect(id.toString()).toBe(hex);
    });

    it('should construct from buffer', () => {
        const hex = '507f1f77bcf86cd799439011';
        const buf = Buffer.from(hex, 'hex');
        const id = new ObjectId(buf);
        expect(id.toHexString()).toBe(hex);
    });

    it('should fail for invalid inputs', () => {
        expect(() => new ObjectId('invalid')).toThrow();
        expect(() => new ObjectId('12345678901234567890123')).toThrow(); // 23 chars
        expect(() => new ObjectId(123)).toThrow();
        expect(() => new ObjectId({})).toThrow();
    });

    it('isValid should work correctly', () => {
        expect(ObjectId.isValid('507f1f77bcf86cd799439011')).toBe(true);
        expect(ObjectId.isValid('invalid')).toBe(false);
        expect(ObjectId.isValid(null)).toBe(false);
        expect(ObjectId.isValid(undefined)).toBe(false);
        expect(ObjectId.isValid(123)).toBe(false);
        expect(ObjectId.isValid(new ObjectId())).toBe(true);
    });

    it('equals should work correctly', () => {
        const hex = '507f1f77bcf86cd799439011';
        const id1 = new ObjectId(hex);
        const id2 = new ObjectId(hex);
        const id3 = new ObjectId();

        expect(id1.equals(id2)).toBe(true);
        expect(id1.equals(hex)).toBe(true);
        expect(id1.equals(id3)).toBe(false);
        expect(id1.equals('other')).toBe(false);
    });

    it('getTimestamp should return correct date', () => {
        // 0x507f1f77 = 1350508407 (seconds) -> 2012-10-17T21:13:27.000Z
        const hex = '507f1f77bcf86cd799439011';
        const id = new ObjectId(hex);
        expect(id.getTimestamp().toISOString()).toBe('2012-10-17T21:13:27.000Z');
    });

    it('createFromTime should create id with correct timestamp', () => {
        const time = 1350430583;
        const id = ObjectId.createFromTime(time);
        expect(id.getTimestamp().getTime()).toBe(time * 1000);
    });

    it('toJSON should return hex string', () => {
        const hex = '507f1f77bcf86cd799439011';
        const id = new ObjectId(hex);
        expect(JSON.stringify(id)).toBe(`"${hex}"`);
    });
});
