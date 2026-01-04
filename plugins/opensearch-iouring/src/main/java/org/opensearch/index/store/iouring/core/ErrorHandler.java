/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.iouring.core;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.DirectoryNotEmptyException;

/**
 * Translates errno values to appropriate Java exceptions.
 *
 * <p>Provides comprehensive mapping from Linux errno codes to the most
 * specific Java exception type, enabling accurate error diagnosis in production.
 *
 * <p>Thread Safety: All methods are stateless and thread-safe.
 */
public final class ErrorHandler {

    private ErrorHandler() {
        // Non-instantiable
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // errno Constants
    // ═══════════════════════════════════════════════════════════════════════════

    public static final int EPERM = 1;           // Operation not permitted
    public static final int ENOENT = 2;          // No such file or directory
    public static final int ESRCH = 3;           // No such process
    public static final int EINTR = 4;           // Interrupted system call
    public static final int EIO = 5;             // I/O error
    public static final int ENXIO = 6;           // No such device or address
    public static final int E2BIG = 7;           // Argument list too long
    public static final int ENOEXEC = 8;         // Exec format error
    public static final int EBADF = 9;           // Bad file descriptor
    public static final int ECHILD = 10;         // No child processes
    public static final int EAGAIN = 11;         // Try again / Resource temporarily unavailable
    public static final int EWOULDBLOCK = 11;    // Same as EAGAIN
    public static final int ENOMEM = 12;         // Out of memory
    public static final int EACCES = 13;         // Permission denied
    public static final int EFAULT = 14;         // Bad address
    public static final int ENOTBLK = 15;        // Block device required
    public static final int EBUSY = 16;          // Device or resource busy
    public static final int EEXIST = 17;         // File exists
    public static final int EXDEV = 18;          // Cross-device link
    public static final int ENODEV = 19;         // No such device
    public static final int ENOTDIR = 20;        // Not a directory
    public static final int EISDIR = 21;         // Is a directory
    public static final int EINVAL = 22;         // Invalid argument
    public static final int ENFILE = 23;         // File table overflow
    public static final int EMFILE = 24;         // Too many open files
    public static final int ENOTTY = 25;         // Not a typewriter
    public static final int ETXTBSY = 26;        // Text file busy
    public static final int EFBIG = 27;          // File too large
    public static final int ENOSPC = 28;         // No space left on device
    public static final int ESPIPE = 29;         // Illegal seek
    public static final int EROFS = 30;          // Read-only file system
    public static final int EMLINK = 31;         // Too many links
    public static final int EPIPE = 32;          // Broken pipe
    public static final int EDOM = 33;           // Math argument out of domain
    public static final int ERANGE = 34;         // Math result not representable
    public static final int EDEADLK = 35;        // Resource deadlock would occur
    public static final int ENAMETOOLONG = 36;   // File name too long
    public static final int ENOLCK = 37;         // No record locks available
    public static final int ENOSYS = 38;         // Function not implemented
    public static final int ENOTEMPTY = 39;      // Directory not empty
    public static final int ELOOP = 40;          // Too many symbolic links
    public static final int ENOMSG = 42;         // No message of desired type
    public static final int EIDRM = 43;          // Identifier removed
    public static final int ENODATA = 61;        // No data available
    public static final int ETIME = 62;          // Timer expired
    public static final int EOVERFLOW = 75;      // Value too large
    public static final int EILSEQ = 84;         // Illegal byte sequence
    public static final int ENOTSOCK = 88;       // Socket operation on non-socket
    public static final int EDESTADDRREQ = 89;   // Destination address required
    public static final int EMSGSIZE = 90;       // Message too long
    public static final int EPROTOTYPE = 91;     // Protocol wrong type for socket
    public static final int ENOPROTOOPT = 92;    // Protocol not available
    public static final int EPROTONOSUPPORT = 93;// Protocol not supported
    public static final int EOPNOTSUPP = 95;     // Operation not supported
    public static final int ENOTSUP = 95;        // Same as EOPNOTSUPP
    public static final int EAFNOSUPPORT = 97;   // Address family not supported
    public static final int EADDRINUSE = 98;     // Address already in use
    public static final int EADDRNOTAVAIL = 99;  // Cannot assign requested address
    public static final int ENETDOWN = 100;      // Network is down
    public static final int ENETUNREACH = 101;   // Network is unreachable
    public static final int ECONNABORTED = 103;  // Connection aborted
    public static final int ECONNRESET = 104;    // Connection reset by peer
    public static final int ENOBUFS = 105;       // No buffer space available
    public static final int EISCONN = 106;       // Transport endpoint already connected
    public static final int ENOTCONN = 107;      // Transport endpoint not connected
    public static final int ETIMEDOUT = 110;     // Connection timed out
    public static final int ECONNREFUSED = 111;  // Connection refused
    public static final int EHOSTDOWN = 112;     // Host is down
    public static final int EHOSTUNREACH = 113;  // No route to host
    public static final int EALREADY = 114;      // Operation already in progress
    public static final int EINPROGRESS = 115;   // Operation now in progress
    public static final int ESTALE = 116;        // Stale file handle
    public static final int EDQUOT = 122;        // Quota exceeded
    public static final int ECANCELED = 125;     // Operation canceled

    // ═══════════════════════════════════════════════════════════════════════════
    // Exception Translation
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Translates an errno to an appropriate IOException.
     *
     * @param errno   Positive errno value
     * @param path    File path for exception message (may be null)
     * @param context Additional context for message (may be null)
     * @return Appropriate IOException subclass
     */
    public static Exception translateError(int errno, String path, String context) {
        String message = buildMessage(errno, path, context);

        return switch (errno) {
            // File/path not found
            case ENOENT -> new NoSuchFileException(path, null, errnoDescription(errno));

            // Permission errors
            case EACCES, EPERM -> new AccessDeniedException(path, null, errnoDescription(errno));

            // File already exists
            case EEXIST -> new FileAlreadyExistsException(path, null, errnoDescription(errno));

            // Directory errors
            case ENOTDIR -> new NotDirectoryException(path);
            case EISDIR -> new FileSystemException(path, null, "Is a directory");
            case ENOTEMPTY -> new DirectoryNotEmptyException(path);

            // Bad file descriptor - channel closed
            case EBADF -> new ClosedChannelException();

            // Invalid argument - programming error
            case EINVAL -> new IllegalArgumentException(message);

            // Resource exhaustion
            case ENOSPC -> new IOException("No space left on device: " + formatPath(path));
            case EDQUOT -> new IOException("Disk quota exceeded: " + formatPath(path));
            case EMFILE -> new IOException("Too many open files in process: " + formatPath(path));
            case ENFILE -> new IOException("Too many open files in system: " + formatPath(path));
            case ENOMEM -> new IOException("Out of memory: " + formatPath(path));

            // File system errors
            case EROFS -> new IOException("Read-only file system: " + formatPath(path));
            case EFBIG -> new IOException("File too large: " + formatPath(path));
            case ENAMETOOLONG -> new IOException("File name too long: " + formatPath(path));
            case ELOOP -> new IOException("Too many symbolic links: " + formatPath(path));
            case ESTALE -> new IOException("Stale file handle: " + formatPath(path));

            // I/O errors
            case EIO -> new IOException("I/O error: " + formatPath(path));
            case ENXIO -> new IOException("No such device or address: " + formatPath(path));

            // Operation errors
            case EINTR -> new IOException("Operation interrupted: " + formatContext(context));
            case EAGAIN -> new IOException("Resource temporarily unavailable: " + formatContext(context));
            case EBUSY -> new IOException("Device or resource busy: " + formatPath(path));
            case ETXTBSY -> new IOException("Text file busy: " + formatPath(path));
            case EPIPE -> new IOException("Broken pipe: " + formatContext(context));
            case ESPIPE -> new IOException("Illegal seek: " + formatPath(path));
            case EXDEV -> new IOException("Cross-device link: " + formatPath(path));

            // Timeout and cancellation
            case ETIME, ETIMEDOUT -> new IOException("Operation timed out: " + formatContext(context));
            case ECANCELED -> new IOException("Operation canceled: " + formatContext(context));

            // Network errors (in case of future socket support)
            case ECONNREFUSED -> new IOException("Connection refused: " + formatContext(context));
            case ECONNRESET -> new IOException("Connection reset: " + formatContext(context));
            case ECONNABORTED -> new IOException("Connection aborted: " + formatContext(context));
            case ENETUNREACH -> new IOException("Network unreachable: " + formatContext(context));
            case EHOSTUNREACH -> new IOException("Host unreachable: " + formatContext(context));

            // Default fallback with full details
            default -> new IOException(message);
        };
    }

    /**
     * Translates errno with path only.
     *
     * @param errno Positive errno value
     * @param path  File path for exception message
     * @return Appropriate Exception subclass
     */
    public static Exception translateError(int errno, String path) {
        return translateError(errno, path, null);
    }

    /**
     * Translates errno with no additional context.
     *
     * @param errno Positive errno value
     * @return Appropriate Exception subclass
     */
    public static Exception translateError(int errno) {
        return translateError(errno, null, null);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Error Information
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Returns the standard name for an errno value.
     *
     * @param errno Positive errno value
     * @return Name like "ENOENT", "EACCES", or "ERRNO_" + value if unknown
     */
    public static String errnoName(int errno) {
        return switch (errno) {
            case EPERM -> "EPERM";
            case ENOENT -> "ENOENT";
            case ESRCH -> "ESRCH";
            case EINTR -> "EINTR";
            case EIO -> "EIO";
            case ENXIO -> "ENXIO";
            case E2BIG -> "E2BIG";
            case ENOEXEC -> "ENOEXEC";
            case EBADF -> "EBADF";
            case ECHILD -> "ECHILD";
            case EAGAIN -> "EAGAIN";
            case ENOMEM -> "ENOMEM";
            case EACCES -> "EACCES";
            case EFAULT -> "EFAULT";
            case ENOTBLK -> "ENOTBLK";
            case EBUSY -> "EBUSY";
            case EEXIST -> "EEXIST";
            case EXDEV -> "EXDEV";
            case ENODEV -> "ENODEV";
            case ENOTDIR -> "ENOTDIR";
            case EISDIR -> "EISDIR";
            case EINVAL -> "EINVAL";
            case ENFILE -> "ENFILE";
            case EMFILE -> "EMFILE";
            case ENOTTY -> "ENOTTY";
            case ETXTBSY -> "ETXTBSY";
            case EFBIG -> "EFBIG";
            case ENOSPC -> "ENOSPC";
            case ESPIPE -> "ESPIPE";
            case EROFS -> "EROFS";
            case EMLINK -> "EMLINK";
            case EPIPE -> "EPIPE";
            case EDOM -> "EDOM";
            case ERANGE -> "ERANGE";
            case EDEADLK -> "EDEADLK";
            case ENAMETOOLONG -> "ENAMETOOLONG";
            case ENOLCK -> "ENOLCK";
            case ENOSYS -> "ENOSYS";
            case ENOTEMPTY -> "ENOTEMPTY";
            case ELOOP -> "ELOOP";
            case ENOMSG -> "ENOMSG";
            case EIDRM -> "EIDRM";
            case ENODATA -> "ENODATA";
            case ETIME -> "ETIME";
            case EOVERFLOW -> "EOVERFLOW";
            case EILSEQ -> "EILSEQ";
            case ENOTSOCK -> "ENOTSOCK";
            case EDESTADDRREQ -> "EDESTADDRREQ";
            case EMSGSIZE -> "EMSGSIZE";
            case EPROTOTYPE -> "EPROTOTYPE";
            case ENOPROTOOPT -> "ENOPROTOOPT";
            case EPROTONOSUPPORT -> "EPROTONOSUPPORT";
            case EOPNOTSUPP -> "EOPNOTSUPP";
            case EAFNOSUPPORT -> "EAFNOSUPPORT";
            case EADDRINUSE -> "EADDRINUSE";
            case EADDRNOTAVAIL -> "EADDRNOTAVAIL";
            case ENETDOWN -> "ENETDOWN";
            case ENETUNREACH -> "ENETUNREACH";
            case ECONNABORTED -> "ECONNABORTED";
            case ECONNRESET -> "ECONNRESET";
            case ENOBUFS -> "ENOBUFS";
            case EISCONN -> "EISCONN";
            case ENOTCONN -> "ENOTCONN";
            case ETIMEDOUT -> "ETIMEDOUT";
            case ECONNREFUSED -> "ECONNREFUSED";
            case EHOSTDOWN -> "EHOSTDOWN";
            case EHOSTUNREACH -> "EHOSTUNREACH";
            case EALREADY -> "EALREADY";
            case EINPROGRESS -> "EINPROGRESS";
            case ESTALE -> "ESTALE";
            case EDQUOT -> "EDQUOT";
            case ECANCELED -> "ECANCELED";
            default -> "ERRNO_" + errno;
        };
    }

    /**
     * Returns a human-readable description for an errno.
     *
     * @param errno Positive errno value
     * @return Description like "No such file or directory"
     */
    public static String errnoDescription(int errno) {
        return switch (errno) {
            case EPERM -> "Operation not permitted";
            case ENOENT -> "No such file or directory";
            case ESRCH -> "No such process";
            case EINTR -> "Interrupted system call";
            case EIO -> "I/O error";
            case ENXIO -> "No such device or address";
            case E2BIG -> "Argument list too long";
            case ENOEXEC -> "Exec format error";
            case EBADF -> "Bad file descriptor";
            case ECHILD -> "No child processes";
            case EAGAIN -> "Resource temporarily unavailable";
            case ENOMEM -> "Out of memory";
            case EACCES -> "Permission denied";
            case EFAULT -> "Bad address";
            case ENOTBLK -> "Block device required";
            case EBUSY -> "Device or resource busy";
            case EEXIST -> "File exists";
            case EXDEV -> "Cross-device link";
            case ENODEV -> "No such device";
            case ENOTDIR -> "Not a directory";
            case EISDIR -> "Is a directory";
            case EINVAL -> "Invalid argument";
            case ENFILE -> "File table overflow";
            case EMFILE -> "Too many open files";
            case ENOTTY -> "Not a typewriter";
            case ETXTBSY -> "Text file busy";
            case EFBIG -> "File too large";
            case ENOSPC -> "No space left on device";
            case ESPIPE -> "Illegal seek";
            case EROFS -> "Read-only file system";
            case EMLINK -> "Too many links";
            case EPIPE -> "Broken pipe";
            case EDOM -> "Math argument out of domain";
            case ERANGE -> "Math result not representable";
            case EDEADLK -> "Resource deadlock would occur";
            case ENAMETOOLONG -> "File name too long";
            case ENOLCK -> "No record locks available";
            case ENOSYS -> "Function not implemented";
            case ENOTEMPTY -> "Directory not empty";
            case ELOOP -> "Too many symbolic links";
            case ENOMSG -> "No message of desired type";
            case EIDRM -> "Identifier removed";
            case ENODATA -> "No data available";
            case ETIME -> "Timer expired";
            case EOVERFLOW -> "Value too large for data type";
            case EILSEQ -> "Illegal byte sequence";
            case ENOTSOCK -> "Socket operation on non-socket";
            case EDESTADDRREQ -> "Destination address required";
            case EMSGSIZE -> "Message too long";
            case EPROTOTYPE -> "Protocol wrong type for socket";
            case ENOPROTOOPT -> "Protocol not available";
            case EPROTONOSUPPORT -> "Protocol not supported";
            case EOPNOTSUPP -> "Operation not supported";
            case EAFNOSUPPORT -> "Address family not supported";
            case EADDRINUSE -> "Address already in use";
            case EADDRNOTAVAIL -> "Cannot assign requested address";
            case ENETDOWN -> "Network is down";
            case ENETUNREACH -> "Network is unreachable";
            case ECONNABORTED -> "Connection aborted";
            case ECONNRESET -> "Connection reset by peer";
            case ENOBUFS -> "No buffer space available";
            case EISCONN -> "Transport endpoint already connected";
            case ENOTCONN -> "Transport endpoint not connected";
            case ETIMEDOUT -> "Connection timed out";
            case ECONNREFUSED -> "Connection refused";
            case EHOSTDOWN -> "Host is down";
            case EHOSTUNREACH -> "No route to host";
            case EALREADY -> "Operation already in progress";
            case EINPROGRESS -> "Operation now in progress";
            case ESTALE -> "Stale file handle";
            case EDQUOT -> "Disk quota exceeded";
            case ECANCELED -> "Operation canceled";
            default -> "Unknown error " + errno;
        };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Retriability
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Checks if the errno indicates a retriable condition.
     *
     * <p>Retriable errors are transient conditions where the same operation
     * might succeed if attempted again after a short delay.
     *
     * @param errno Positive errno value
     * @return true if operation might succeed on retry
     */
    public static boolean isRetriable(int errno) {
        return switch (errno) {
            case EAGAIN,    // Resource temporarily unavailable
                 EINTR,     // Interrupted system call
                 EBUSY,     // Device or resource busy
                 ENOMEM,    // Out of memory (might free up)
                 ENOBUFS,   // No buffer space available
                 EALREADY,  // Operation already in progress
                 EINPROGRESS // Operation now in progress
                -> true;
            default -> false;
        };
    }

    /**
     * Checks if the errno indicates a permanent/fatal error.
     *
     * <p>Permanent errors indicate conditions that won't change without
     * external intervention (e.g., missing file, permission denied).
     *
     * @param errno Positive errno value
     * @return true if retrying would not help
     */
    public static boolean isPermanent(int errno) {
        return switch (errno) {
            case ENOENT,    // No such file or directory
                 EACCES,    // Permission denied
                 EPERM,     // Operation not permitted
                 EBADF,     // Bad file descriptor
                 EINVAL,    // Invalid argument
                 ENOSPC,    // No space left on device
                 EROFS,     // Read-only file system
                 EEXIST,    // File exists
                 ENOTDIR,   // Not a directory
                 EISDIR,    // Is a directory
                 ENOTEMPTY, // Directory not empty
                 EFBIG,     // File too large
                 ENOSYS,    // Function not implemented
                 ENAMETOOLONG, // File name too long
                 ELOOP,     // Too many symbolic links
                 EDQUOT     // Disk quota exceeded
                -> true;
            default -> false;
        };
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Message Building Helpers
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Builds a complete error message with all available context.
     */
    private static String buildMessage(int errno, String path, String context) {
        StringBuilder sb = new StringBuilder();

        // Start with errno info
        sb.append(errnoName(errno))
            .append(": ")
            .append(errnoDescription(errno));

        // Add path if available
        if (path != null && !path.isEmpty()) {
            sb.append(" [path: ").append(path).append("]");
        }

        // Add context if available
        if (context != null && !context.isEmpty()) {
            sb.append(" [").append(context).append("]");
        }

        // Add errno number for debugging
        sb.append(" (errno=").append(errno).append(")");

        return sb.toString();
    }

    /**
     * Formats path for display, handling null.
     */
    private static String formatPath(String path) {
        return (path != null && !path.isEmpty()) ? path : "<unknown>";
    }

    /**
     * Formats context for display, handling null.
     */
    private static String formatContext(String context) {
        return (context != null && !context.isEmpty()) ? context : "<unknown>";
    }
}
