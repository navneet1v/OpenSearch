/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring.core;

import org.junit.Test;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.DirectoryNotEmptyException;

import static org.junit.Assert.*;

public class ErrorHandlerTests {

    @Test
    public void testTranslateEnoent() {
        Exception e = ErrorHandler.translateError(ErrorHandler.ENOENT, "/test/path");
        assertTrue(e instanceof NoSuchFileException);
    }

    @Test
    public void testTranslateEacces() {
        Exception e = ErrorHandler.translateError(ErrorHandler.EACCES, "/test/path");
        assertTrue(e instanceof AccessDeniedException);
    }

    @Test
    public void testTranslateEperm() {
        Exception e = ErrorHandler.translateError(ErrorHandler.EPERM, "/test/path");
        assertTrue(e instanceof AccessDeniedException);
    }

    @Test
    public void testTranslateEexist() {
        Exception e = ErrorHandler.translateError(ErrorHandler.EEXIST, "/test/path");
        assertTrue(e instanceof FileAlreadyExistsException);
    }

    @Test
    public void testTranslateEnotdir() {
        Exception e = ErrorHandler.translateError(ErrorHandler.ENOTDIR, "/test/path");
        assertTrue(e instanceof NotDirectoryException);
    }

    @Test
    public void testTranslateEnotempty() {
        Exception e = ErrorHandler.translateError(ErrorHandler.ENOTEMPTY, "/test/path");
        assertTrue(e instanceof DirectoryNotEmptyException);
    }

    @Test
    public void testTranslateEbadf() {
        Exception e = ErrorHandler.translateError(ErrorHandler.EBADF, "/test/path");
        assertTrue(e instanceof ClosedChannelException);
    }

    @Test
    public void testTranslateEinval() {
        Exception e = ErrorHandler.translateError(ErrorHandler.EINVAL, "/test/path");
        assertTrue(e instanceof IllegalArgumentException);
    }

    @Test
    public void testTranslateEio() {
        Exception e = ErrorHandler.translateError(ErrorHandler.EIO, "/test/path");
        assertTrue(e instanceof IOException);
        assertTrue(e.getMessage().contains("I/O error"));
    }

    @Test
    public void testTranslateEnospc() {
        Exception e = ErrorHandler.translateError(ErrorHandler.ENOSPC, "/test/path");
        assertTrue(e instanceof IOException);
        assertTrue(e.getMessage().contains("No space left"));
    }

    @Test
    public void testTranslateUnknownErrno() {
        Exception e = ErrorHandler.translateError(9999, "/test/path", "context");
        assertTrue(e instanceof IOException);
        assertTrue(e.getMessage().contains("9999"));
    }

    @Test
    public void testTranslateWithContext() {
        Exception e = ErrorHandler.translateError(ErrorHandler.EINTR, "/test/path", "read operation");
        assertTrue(e instanceof IOException);
        assertTrue(e.getMessage().contains("read operation"));
    }

    @Test
    public void testErrnoNameKnown() {
        assertEquals("ENOENT", ErrorHandler.errnoName(ErrorHandler.ENOENT));
        assertEquals("EACCES", ErrorHandler.errnoName(ErrorHandler.EACCES));
        assertEquals("EINVAL", ErrorHandler.errnoName(ErrorHandler.EINVAL));
        assertEquals("EAGAIN", ErrorHandler.errnoName(ErrorHandler.EAGAIN));
        assertEquals("EIO", ErrorHandler.errnoName(ErrorHandler.EIO));
    }

    @Test
    public void testErrnoNameUnknown() {
        String name = ErrorHandler.errnoName(9999);
        assertEquals("ERRNO_9999", name);
    }

    @Test
    public void testErrnoDescriptionKnown() {
        assertEquals("No such file or directory", ErrorHandler.errnoDescription(ErrorHandler.ENOENT));
        assertEquals("Permission denied", ErrorHandler.errnoDescription(ErrorHandler.EACCES));
        assertEquals("Invalid argument", ErrorHandler.errnoDescription(ErrorHandler.EINVAL));
        assertEquals("I/O error", ErrorHandler.errnoDescription(ErrorHandler.EIO));
    }

    @Test
    public void testErrnoDescriptionUnknown() {
        String desc = ErrorHandler.errnoDescription(9999);
        assertTrue(desc.contains("Unknown error"));
    }

    @Test
    public void testIsRetriableEagain() {
        assertTrue(ErrorHandler.isRetriable(ErrorHandler.EAGAIN));
    }

    @Test
    public void testIsRetriableEintr() {
        assertTrue(ErrorHandler.isRetriable(ErrorHandler.EINTR));
    }

    @Test
    public void testIsRetriableEbusy() {
        assertTrue(ErrorHandler.isRetriable(ErrorHandler.EBUSY));
    }

    @Test
    public void testIsRetriableEnomem() {
        assertTrue(ErrorHandler.isRetriable(ErrorHandler.ENOMEM));
    }

    @Test
    public void testIsNotRetriableEnoent() {
        assertFalse(ErrorHandler.isRetriable(ErrorHandler.ENOENT));
    }

    @Test
    public void testIsNotRetriableEacces() {
        assertFalse(ErrorHandler.isRetriable(ErrorHandler.EACCES));
    }

    @Test
    public void testIsPermanentEnoent() {
        assertTrue(ErrorHandler.isPermanent(ErrorHandler.ENOENT));
    }

    @Test
    public void testIsPermanentEacces() {
        assertTrue(ErrorHandler.isPermanent(ErrorHandler.EACCES));
    }

    @Test
    public void testIsPermanentEinval() {
        assertTrue(ErrorHandler.isPermanent(ErrorHandler.EINVAL));
    }

    @Test
    public void testIsPermanentEnospc() {
        assertTrue(ErrorHandler.isPermanent(ErrorHandler.ENOSPC));
    }

    @Test
    public void testIsNotPermanentEagain() {
        assertFalse(ErrorHandler.isPermanent(ErrorHandler.EAGAIN));
    }

    @Test
    public void testIsNotPermanentEintr() {
        assertFalse(ErrorHandler.isPermanent(ErrorHandler.EINTR));
    }

    @Test
    public void testTranslateWithNullPath() {
        Exception e = ErrorHandler.translateError(ErrorHandler.EIO);
        assertTrue(e instanceof IOException);
    }

    @Test
    public void testTranslateNetworkErrors() {
        assertTrue(ErrorHandler.translateError(ErrorHandler.ECONNREFUSED) instanceof IOException);
        assertTrue(ErrorHandler.translateError(ErrorHandler.ECONNRESET) instanceof IOException);
        assertTrue(ErrorHandler.translateError(ErrorHandler.ETIMEDOUT) instanceof IOException);
    }
}
