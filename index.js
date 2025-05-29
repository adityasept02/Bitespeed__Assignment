const express = require("express");
const pool = require("./db");
require("dotenv").config();

const app = express();
app.use(express.json());

app.get("/", (req, res) => {
  res.send('Please do go on the "/identify" route and use Post method to check the tests. Thank you!');
});

app.post("/identify", async (req, res) => {
  const { email, phoneNumber } = req.body;
  if (!email && !phoneNumber) {
    return res.status(400).json({ error: "Email or phoneNumber is required" });
  }

  try {
    const matchQuery = `
      SELECT * FROM contacts
      WHERE email = $1 OR phoneNumber = $2
    `;
    const { rows: matches } = await pool.query(matchQuery, [email, phoneNumber]);

    if (matches.length === 0) {
      const insertQuery = `
        INSERT INTO contacts (email, phoneNumber, linkPrecedence)
        VALUES ($1, $2, 'primary')
        RETURNING *
      `;
      const { rows } = await pool.query(insertQuery, [email, phoneNumber]);
      const newContact = rows[0];

      return res.status(200).json({
        contact: {
          primaryContactId: newContact.id,
          emails: newContact.email ? [newContact.email] : [],
          phoneNumbers: newContact.phonenumber ? [newContact.phonenumber] : [],
          secondaryContactIds: [],
        },
      });
    }

    const primaryIds = [...new Set(matches.map(c =>
      c.linkprecedence === "primary" ? c.id : c.linkedid
    ))];

    const linkQuery = `
      SELECT * FROM contacts
      WHERE id = ANY($1) OR linkedId = ANY($1)
    `;
    const { rows: linked } = await pool.query(linkQuery, [primaryIds]);

    const contactsMap = {};
    [...matches, ...linked].forEach(c => (contactsMap[c.id] = c));
    const allContacts = Object.values(contactsMap);

    const primary = allContacts.reduce((a, b) =>
      new Date(a.createdat) < new Date(b.createdat) ? a : b
    );

    await Promise.all(
      allContacts
        .filter(c => c.id !== primary.id && c.linkprecedence === "primary")
        .map(c =>
          pool.query(
            `
            UPDATE contacts
            SET linkPrecedence = 'secondary', linkedId = $1, updatedAt = CURRENT_TIMESTAMP
            WHERE id = $2
          `,
            [primary.id, c.id]
          )
        )
    );

    const emails = [...new Set(allContacts.map(c => c.email).filter(Boolean))];
    const phones = [...new Set(allContacts.map(c => c.phonenumber).filter(Boolean))];

    const isNew =
      (email && !emails.includes(email)) ||
      (phoneNumber && !phones.includes(phoneNumber));

    if (isNew) {
      await pool.query(
        `
        INSERT INTO contacts (email, phoneNumber, linkPrecedence, linkedId)
        VALUES ($1, $2, 'secondary', $3)
      `,
        [email, phoneNumber, primary.id]
      );
    }

    const { rows: final } = await pool.query(
      `SELECT * FROM contacts WHERE id = $1 OR linkedId = $1`,
      [primary.id]
    );

    return res.status(200).json({
      contact: {
        primaryContactId: primary.id,
        emails: [...new Set(final.map(c => c.email).filter(Boolean))],
        phoneNumbers: [...new Set(final.map(c => c.phonenumber).filter(Boolean))],
        secondaryContactIds: final
          .filter(c => c.linkprecedence === "secondary")
          .map(c => c.id),
      },
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
