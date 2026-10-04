CREATE TABLE shows (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL CHECK (length(btrim(name)) > 0),
    price_paise bigint NOT NULL CHECK (price_paise >= 0),
    per_user_limit integer NOT NULL DEFAULT 4 CHECK (per_user_limit > 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE reservations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    show_id uuid NOT NULL REFERENCES shows (id),
    user_id text NOT NULL CHECK (length(user_id) > 0),
    status text NOT NULL CHECK (status IN ('held', 'confirmed', 'cancelled')),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (id, show_id)
);

CREATE TABLE seats (
    show_id uuid NOT NULL REFERENCES shows (id),
    seat_label text NOT NULL CHECK (length(seat_label) > 0),
    status text NOT NULL CHECK (status IN ('available', 'held', 'confirmed')),
    reservation_id uuid,
    PRIMARY KEY (show_id, seat_label),
    FOREIGN KEY (reservation_id, show_id) REFERENCES reservations (id, show_id),
    CHECK (
        (status = 'available' AND reservation_id IS NULL)
        OR (status IN ('held', 'confirmed') AND reservation_id IS NOT NULL)
    )
);

CREATE TABLE reservation_seats (
    reservation_id uuid NOT NULL,
    show_id uuid NOT NULL,
    seat_label text NOT NULL CHECK (length(seat_label) > 0),
    PRIMARY KEY (reservation_id, seat_label),
    FOREIGN KEY (reservation_id, show_id) REFERENCES reservations (id, show_id),
    FOREIGN KEY (show_id, seat_label) REFERENCES seats (show_id, seat_label)
);

CREATE TABLE idempotency_keys (
    user_id text NOT NULL CHECK (length(user_id) > 0),
    show_id uuid NOT NULL REFERENCES shows (id),
    idempotency_key text NOT NULL CHECK (length(idempotency_key) > 0),
    request_hash text NOT NULL CHECK (length(request_hash) > 0),
    outcome text NOT NULL CHECK (
        outcome IN (
            'in_progress',
            'confirmed',
            'seat_taken',
            'per_user_limit',
            'idempotency_mismatch'
        )
    ),
    response jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, show_id, idempotency_key),
    CHECK (
        (outcome = 'in_progress' AND response IS NULL)
        OR (outcome <> 'in_progress' AND response IS NOT NULL)
    )
);

CREATE TABLE user_show_counts (
    user_id text NOT NULL CHECK (length(user_id) > 0),
    show_id uuid NOT NULL REFERENCES shows (id),
    confirmed_seat_count integer NOT NULL DEFAULT 0 CHECK (confirmed_seat_count >= 0),
    PRIMARY KEY (user_id, show_id)
);
