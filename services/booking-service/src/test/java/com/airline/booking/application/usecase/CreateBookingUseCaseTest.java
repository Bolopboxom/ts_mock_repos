package com.airline.booking.application.usecase;

import com.airline.booking.application.dto.BookingDto;
import com.airline.booking.domain.model.Booking;
import com.airline.booking.domain.repository.BookingRepository;
import com.airline.booking.infrastructure.kafka.producer.BookingEventProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
class CreateBookingUseCaseTest {

    @Mock
    private BookingRepository bookingRepository;

    @Mock
    private BookingEventProducer bookingEventProducer;

    @InjectMocks
    private CreateBookingUseCase createBookingUseCase;

    private BookingDto validDto;
    private String testCorrelationId;

    @BeforeEach
    void setUp() {
        validDto = new BookingDto();
        validDto.customerId = "CUST-1";
        validDto.flightId = "FL-100";
        validDto.passengers = 2;
        validDto.usePoints = 50;
        
        testCorrelationId = "corr-test-123";
    }

    @Test
    void execute_shouldCreateBookingAndPublishEvent() {
        // When
        String bookingId = createBookingUseCase.execute(validDto, testCorrelationId);

        // Then
        assertThat(bookingId).isNotNull();
        assertThat(bookingId).startsWith("BKG-");
        assertThat(bookingId).hasSize(12); // "BKG-" + 8 chars

        verify(bookingRepository, times(1)).save(any(Booking.class));
        verify(bookingEventProducer, times(1)).publishBookingCreated(anyString(), eq(testCorrelationId));
    }

    @Test
    void execute_shouldSaveBookingWithCorrectData() {
        // Given
        ArgumentCaptor<Booking> bookingCaptor = ArgumentCaptor.forClass(Booking.class);

        // When
        String bookingId = createBookingUseCase.execute(validDto, testCorrelationId);

        // Then
        verify(bookingRepository).save(bookingCaptor.capture());
        Booking savedBooking = bookingCaptor.getValue();

        assertThat(savedBooking.getBookingId()).isEqualTo(bookingId);
        assertThat(savedBooking.getBookingId()).startsWith("BKG-");
        assertThat(savedBooking.getCustomerId()).isEqualTo("CUST-1");
        assertThat(savedBooking.getFlightId()).isEqualTo("FL-100");
        assertThat(savedBooking.getStatus()).isEqualTo(Booking.Status.PENDING);
    }

    @Test
    void execute_shouldPublishEventWithCorrectPayload() {
        // Given
        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);

        // When
        String bookingId = createBookingUseCase.execute(validDto, testCorrelationId);

        // Then
        verify(bookingEventProducer).publishBookingCreated(payloadCaptor.capture(), eq(testCorrelationId));
        String payload = payloadCaptor.getValue();

        assertThat(payload).contains("\"bookingId\":\"" + bookingId + "\"");
        assertThat(payload).contains("\"customerId\":\"CUST-1\"");
        assertThat(payload).contains("\"flightId\":\"FL-100\"");
        assertThat(payload).contains("\"passengers\":2");
        assertThat(payload).contains("\"usePoints\":50");
    }

    @Test
    void execute_shouldGenerateUniqueBookingIds() {
        // When
        String bookingId1 = createBookingUseCase.execute(validDto, "corr-1");
        String bookingId2 = createBookingUseCase.execute(validDto, "corr-2");
        String bookingId3 = createBookingUseCase.execute(validDto, "corr-3");

        // Then
        assertThat(bookingId1).isNotEqualTo(bookingId2);
        assertThat(bookingId2).isNotEqualTo(bookingId3);
        assertThat(bookingId1).isNotEqualTo(bookingId3);
    }

    @Test
    void execute_shouldUseProvidedCorrelationId() {
        // Given
        String customCorrelationId = "custom-correlation-id-456";

        // When
        createBookingUseCase.execute(validDto, customCorrelationId);

        // Then
        verify(bookingEventProducer).publishBookingCreated(anyString(), eq(customCorrelationId));
    }

    @Test
    void execute_shouldHandleZeroPointsAndPassengers() {
        // Given
        validDto.passengers = 1;
        validDto.usePoints = 0;
        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);

        // When
        createBookingUseCase.execute(validDto, testCorrelationId);

        // Then
        verify(bookingEventProducer).publishBookingCreated(payloadCaptor.capture(), anyString());
        String payload = payloadCaptor.getValue();

        assertThat(payload).contains("\"passengers\":1");
        assertThat(payload).contains("\"usePoints\":0");
    }

    @Test
    void execute_shouldHandleLargePointsValue() {
        // Given
        validDto.usePoints = 999999L;
        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);

        // When
        createBookingUseCase.execute(validDto, testCorrelationId);

        // Then
        verify(bookingEventProducer).publishBookingCreated(payloadCaptor.capture(), anyString());
        String payload = payloadCaptor.getValue();

        assertThat(payload).contains("\"usePoints\":999999");
    }
}
